package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/emersion/go-imap"
	"github.com/emersion/go-imap/client"
	"github.com/emersion/go-message/charset"
)

type Downloader struct {
	Client          *client.Client
	Options         *Options
	currentMailbox  string
	downloadedCount int
	skipedMails     []string
	mutex           sync.Mutex // 保护并发访问共享资源
	bufPool         sync.Pool  // 缓冲区池，用于复用内存
}

func NewDownloader(opts *Options) (d *Downloader, err error) {
	d = &Downloader{}
	d.Options = opts
	// 初始化缓冲区池，减少内存分配
	d.bufPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 32*1024) // 32KB缓冲区
		},
	}
	// 增强邮件编码探测能力
	imap.CharsetReader = charset.Reader
	cli, err := client.DialTLS(d.Options.Host, nil)
	if err != nil {
		return
	}
	d.Client = cli
	log.Info("已连接到服务器:", d.Options.Host)

	if err = d.Client.Login(d.Options.Username, d.Options.Password); err != nil {
		return
	}
	log.Info("已登录:", d.Options.Username)

	return
}

// 下载邮箱
func (d *Downloader) downloadAccountMailbox(ctx context.Context, mailbox string) (err error) {

	d.currentMailbox = mailbox
	status, err := d.Client.Select(mailbox, true)
	if err != nil {
		return
	}
	log.Infof("当前邮箱文件夹%s,总数%d \n", status.Name, status.Messages)

	if status.Messages == 0 {
		return
	}
	all := status.Messages
	dir := filepath.Join(d.Options.absDir, mailbox)
	log.Infof("%s邮箱文件夹下载存放位置: %s\n", mailbox, dir)
	count := int(all / 500)
	t1 := time.Now()
	for i := 0; i <= count; i++ {
		start := i*500 + 1
		end := (i + 1) * 500
		if int(all)-start < 500 {
			end = int(status.Messages)
		}
		log.Infof("\n\n正在分析第%d批:[%d~%d]\n\n", i+1, start, end)
		err = d.downloadMailsByRange(ctx, uint32(start), uint32(end))
		if err != nil {
			log.Errorf("第%d批[%d~%d]下载出错：%s，跳过继续\n", i+1, start, end, err.Error())
		}
	}
	t2 := time.Since(t1)
	log.Infof("下载耗时：%0.0f分钟", t2.Minutes())
	return
}

// 获取匹配前缀的邮箱文件夹
func (d *Downloader) getPrefixMatchedMailBoxes(ctx context.Context) (mailboxes []string, err error) {
	chBoxes := make(chan *imap.MailboxInfo)
	done := make(chan error, 1)
	go func() {
		done <- d.Client.List("", "*", chBoxes)
	}()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case err = <-done:
			log.Infof("枚举邮箱文件夹结束")
			return
		case box := <-chBoxes:
			if box == nil {
				continue
			}
			log.Infof("发现邮箱文件夹: %s", box.Name)
			for _, prefix := range d.Options.Prefixes {
				if strings.HasPrefix(box.Name, prefix) {
					log.Infof("符合前缀条件: %s", box.Name)
					mailboxes = append(mailboxes, box.Name)
					break
				}
			}
		}
	}
}

// 按批次分析并下载邮件
func (d *Downloader) downloadMailsByRange(ctx context.Context, start, end uint32) (err error) {
	seqDL, err := d.getDownloadMailList(ctx, start, end)
	if err != nil {
		log.Errorf("[%d~%d]分析下载队列出错:%s\n", start, end, err.Error())
		for {
			err = d.reconnect()
			if err == nil {
				break
			}
		}
		seqDL, err = d.getDownloadMailList(ctx, start, end)
	}
	if seqDL.Empty() {
		log.Infof("[%d~%d]下载队列为空,跳过:\n", start, end)
		return
	}
	err = d.downloadMailList(ctx, seqDL)

	if err != nil {
		log.Errorf("[%d~%d]下载队列出错:%s\n", start, end, err.Error())
		for {
			err = d.reconnect()
			if err == nil {
				break
			}
		}
		err = d.downloadMailList(ctx, seqDL)
	}
	return
}

// 获取下载列表
// 下载列表填充规则：若本地已存在文件，则跳过
func (d *Downloader) getDownloadMailList(ctx context.Context, start uint32, end uint32) (seqDL *imap.SeqSet, err error) {
	seq := new(imap.SeqSet)
	seq.AddRange(start, end)

	chMsg := make(chan *imap.Message, 500)
	done := make(chan error, 1)

	go func() {
		done <- d.Client.Fetch(seq, []imap.FetchItem{imap.FetchEnvelope, imap.FetchUid}, chMsg)
	}()

	seqDL = new(imap.SeqSet)
	for {
		select {
		case <-ctx.Done():
			return seqDL, ctx.Err()
		case err = <-done:
			// 继续处理chMsg通道中的剩余邮件
			for msg := range chMsg {
				if msg != nil {
					log.Infof("分析邮件: %s\n", msg.Envelope.Subject) // 降级为Debug级别，减少日志输出
					existed, err := d.checkMailStorePathExisted(msg)
					if err != nil {
						log.Errorf("检测路径出错: %s\n", err)
						// 跳过当前邮件，继续处理其他邮件
						continue
					}
					if !existed {
						seqDL.AddNum(msg.Uid)
					}
				}
			}
			return
		case msg := <-chMsg:
			if msg != nil {
				log.Infof("分析邮件: %s\n", msg.Envelope.Subject) // 降级为Debug级别，减少日志输出
				existed, err := d.checkMailStorePathExisted(msg)
				if err != nil {
					log.Errorf("检测路径出错: %s\n", err)
					// 跳过当前邮件，继续处理其他邮件
					continue
				}
				if !existed {
					seqDL.AddNum(msg.Uid)
				}

			}
		}
	}
}

// 下载邮件列表
func (d *Downloader) downloadMailList(ctx context.Context, seqDL *imap.SeqSet) (err error) {
	log.Infof("开始下载队列: %s", seqDL.String())
	chMsg := make(chan *imap.Message, 500)
	done := make(chan error, 1)

	var section imap.BodySectionName

	go func() {
		done <- d.Client.UidFetch(seqDL, []imap.FetchItem{imap.FetchEnvelope, section.FetchItem()}, chMsg)
	}()

	// 并发下载控制
	var wg sync.WaitGroup
	sem := make(chan struct{}, 10) // 限制同时下载10个邮件（增加并发数提高速度）

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err = <-done:
			// 继续处理chMsg通道中的剩余邮件
			for msg := range chMsg {
				if msg != nil {
					wg.Add(1)
					sem <- struct{}{}
					go func(m *imap.Message) {
						defer wg.Done()
						defer func() { <-sem }()

						if err = d.downloadMail(m); err != nil {
							log.Errorf("邮件%s下载出错：%s，跳过继续\n", m.Envelope.Subject, err.Error())
							d.skipedMails = append(d.skipedMails, m.Envelope.Subject)
						}
					}(msg)
				}
			}
			// 等待所有下载完成
			wg.Wait()
			return
		case msg := <-chMsg:
			if msg != nil {
				wg.Add(1)
				sem <- struct{}{}
				go func(m *imap.Message) {
					defer wg.Done()
					defer func() { <-sem }()

					if err = d.downloadMail(m); err != nil {
						log.Errorf("邮件%s下载出错：%s，跳过继续\n", m.Envelope.Subject, err.Error())
						d.skipedMails = append(d.skipedMails, m.Envelope.Subject)
					}
				}(msg)
			}
		}
	}
}

// 下载邮件
func (d *Downloader) downloadMail(msg *imap.Message) (err error) {
	file := d.getMailStorePath(msg)
	log.Infof("存储邮件：%s\n", file) // 降级为Debug级别，减少日志输出

	// 优化文件目录创建（减少权限检查）
	if err = os.MkdirAll(filepath.Dir(file), 0755); err != nil {
		return
	}

	r := msg.GetBody(&imap.BodySectionName{})
	if r == nil {
		log.Errorf("message does not have a body: %s", msg.Envelope.MessageId)
		return
	}

	// 优化文件打开方式（使用更高效的标志）
	var f *os.File
	if f, err = os.OpenFile(file, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644); err != nil {
		return
	}

	defer func(f *os.File) {
		err := f.Close()
		if err != nil {
			log.Errorf("清理文件报错:%s\n", err.Error())
		}
	}(f)

	// 从缓冲区池获取缓冲区，减少内存分配
	buf := d.bufPool.Get().([]byte)
	defer d.bufPool.Put(buf) // 使用完后放回池中

	if _, err = io.CopyBuffer(f, r, buf); err != nil {
		return
	}

	// 原子更新计数器（增加互斥锁保护）
	d.mutex.Lock()
	d.downloadedCount++
	d.mutex.Unlock()

	return
}

// 获取邮件存储路径
func (d *Downloader) getMailStorePath(msg *imap.Message) string {
	year := msg.Envelope.Date.Format("2006")
	month := msg.Envelope.Date.Format("01")

	// 使用strings.Map一次性替换所有特殊字符，提高效率
	subject := strings.Map(func(r rune) rune {
		// 定义需要移除的字符集
		badChars := "“”\"（） 。，【】：:/、<>*\\?|\t\r\n'"
		for _, c := range badChars {
			if r == c {
				return -1 // -1表示移除该字符
			}
		}
		return r // 保留其他字符
	}, msg.Envelope.Subject)

	dir := filepath.Join(d.Options.absDir, d.currentMailbox)
	tid := fmt.Sprintf("%d", msg.Envelope.Date.UnixMilli())
	return filepath.Join(dir, year, month, fmt.Sprintf("%s-%s.eml", subject, tid))
}

// 检查邮件存储路径是否存在
func (d *Downloader) checkMailStorePathExisted(msg *imap.Message) (existed bool, err error) {
	file := d.getMailStorePath(msg)
	exists, err := PathExists(file)
	if err != nil {
		return
	}
	if exists {
		log.Infof("× 跳过：%s", msg.Envelope.Subject)
		return exists, nil
	}
	log.Infof("√ 下载：%s", msg.Envelope.Subject)
	return
}

// 重建连接
func (d *Downloader) reconnect() error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	// 关闭旧连接
	if d.Client != nil {
		if err := d.Client.Logout(); err != nil {
			log.Errorf("登出出错: %s\n", err.Error())
		}
		// 设置为nil，防止其他goroutine继续使用已关闭的连接
		d.Client = nil
	}

	// 建立新连接
	cli, err := client.DialTLS(d.Options.Host, nil)
	if err != nil {
		return err
	}

	// 登录新连接
	if err = cli.Login(d.Options.Username, d.Options.Password); err != nil {
		cli.Logout()
		return err
	}

	// 替换Client
	d.Client = cli
	log.Info("已连接到服务器:", d.Options.Host)
	log.Info("已登录:", d.Options.Username)

	return nil
}
