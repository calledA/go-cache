package logger

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	"github.com/sirupsen/logrus"
)

var (
	path = ""
)

const (
	DefaultCallerDepth = 1
)

var logger *logrus.Logger

func init() {
	logger = logrus.New()
	//显示行号
	// logger.SetReportCaller(true)
	//设置日志级别
	logger.SetLevel(logrus.InfoLevel)
	//设置格式
	logger.SetFormatter(&logrus.JSONFormatter{
		TimestampFormat: "2006-01-02 15:04:05",
	})
	//设置标准错误
	// logger.SetOutput(os.Stdout)
	//输出到mongoDB
	// hooker, err := mgorus.NewHooker("localhost:27017", "gmr-logs", time.Now().Format("2006-01-02"))
	// if err == nil {
	// 	logger.AddHook(hooker)
	// } else {
	// 	logger.Error(err)
	// }
	// 添加hook
	//logger.AddHook(&EmailAlarmHook{})
	path, err := os.Getwd()
	if err != nil {
		logger.Error(err)
	}
	path = filepath.Join(path, "/logs")
	writer, _ := rotatelogs.New(
		path+"/%Y%m%d"+".log",
		// WithRotationTime设置日志分割的时间，这里设置为24小时分割一次
		rotatelogs.WithRotationTime(24*time.Hour),

		// WithMaxAge和WithRotationCount二者只能设置一个，
		// WithMaxAge设置文件清理前的最长保存时间，
		// WithRotationCount设置文件清理前最多保存的个数。
		//	rotatelogs.WithMaxAge(time.Hour*24),
		rotatelogs.WithMaxAge(7*24*time.Hour),
	)
	logger.SetOutput(writer)
}

func Print(args ...interface{}) {
	logger.WithFields(logrus.Fields{
		"file": setFileLine(),
	}).Print(args...)
}

func Printf(format string, args ...interface{}) {
	logger.WithFields(logrus.Fields{
		"file": setFileLine(),
	}).Printf(format, args...)
}

func Debug(args ...interface{}) {
	logger.WithFields(logrus.Fields{
		"file": setFileLine(),
	}).Debug(args...)
}

func Debugf(format string, args ...interface{}) {
	logger.WithFields(logrus.Fields{
		"file": setFileLine(),
	}).Debugf(format, args...)
}

func Info(args ...interface{}) {
	logger.WithFields(logrus.Fields{
		"file": setFileLine(),
	}).Info(args...)
}

func Infof(format string, args ...interface{}) {
	logger.WithFields(logrus.Fields{
		"file": setFileLine(),
	}).Infof(format, args...)
}

func Warn(args ...interface{}) {
	logger.WithFields(logrus.Fields{
		"file": setFileLine(),
	}).Warn(args...)
}

func Warnf(format string, args ...interface{}) {
	logger.WithFields(logrus.Fields{
		"file": setFileLine(),
	}).Warnf(format, args...)
}

func Error(args ...interface{}) {
	logger.WithFields(logrus.Fields{
		"file": setFileLine(),
	}).Error(args...)
}

func Errorf(format string, args ...interface{}) {
	logger.WithFields(logrus.Fields{
		"file": setFileLine(),
	}).Errorf(format, args...)
}

func Fatal(args ...interface{}) {
	logger.WithFields(logrus.Fields{
		"file": setFileLine(),
	}).Fatal(args...)
}

func Fatalf(format string, args ...interface{}) {
	logger.WithFields(logrus.Fields{
		"file": setFileLine(),
	}).Fatalf(format, args...)
}

func Panic(args ...interface{}) {
	logger.WithFields(logrus.Fields{
		"file": setFileLine(),
	}).Panic(args...)
}

func Panicf(format string, args ...interface{}) {
	logger.WithFields(logrus.Fields{
		"file": setFileLine(),
	}).Panicf(format, args...)
}

func Trace(args ...interface{}) {
	logger.WithFields(logrus.Fields{
		"file": setFileLine(),
	}).Trace(args...)
}

func Tracef(format string, args ...interface{}) {
	logger.WithFields(logrus.Fields{
		"file": setFileLine(),
	}).Tracef(format, args...)
}

func setFileLine() (filePath string) {
	_, file, line, ok := runtime.Caller(2)
	abs, _ := filepath.Abs(file)
	root, _ := os.Getwd()
	path := strings.Replace(strings.Replace(abs, root, "", 2), "\\", "/", -1)
	if ok {
		filePath = fmt.Sprintf("%s:%d", path, line)
	} else {
		filePath = "file path not found"
	}
	return
}
