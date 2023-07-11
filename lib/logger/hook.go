package logger

import (
	"github.com/sirupsen/logrus"
)

type EmailAlarmHook struct{}

func (hook *EmailAlarmHook) Fire(entry *logrus.Entry) error {
	// entry.Data["app"] = "GMR-SERVER"
	if entry.Level == logrus.FatalLevel {
		// SendMail()
	}
	return nil
}

func (hook *EmailAlarmHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

func SendMail() {
	//mail := gomail.NewMessage()
	//mail.SetAddressHeader("From", "iamwzxin@163.com", "test") // 发件人邮箱，发件人名称
	//mail.SetHeader("To", "help@bigpanda.app")
	//mail.SetHeader("Subject", "test")                                                 // 主题
	//mail.SetBody("text/html", "正文")                                                   // 正文
	//d := gomail.NewDialer("smtp.163.com", 25, "iamwzxin@163.com", "TJYYURVFJWTAQJLJ") // 发送邮件服务器、端口、发件人账号、（授权秘钥）
	//if err := d.DialAndSend(mail); err != nil {
	//	logger.Error("send mail err:", err)
	//}
}
