package log

import (
	"fmt"
	"log"
	"os"
	"strings"
)

const SEQUINS_LOG_LINE = "CANONICAL-SEQUINS-LINE"

type KeyValue map[string]interface{}

func (x KeyValue) String() string {
	s := make([]string, 0, len(x))
	for k, v := range x {
		s = append(s, fmt.Sprintf("%s=%q", k, v))
	}

	return fmt.Sprintf("%s: %s", SEQUINS_LOG_LINE, strings.Join(s, " "))
}

func Println(v ...interface{}) {
	log.Println(KeyValue{"msg": fmt.Sprint(v...)})
}

func Printf(format string, v ...interface{}) {
	Println(fmt.Sprintf(format, v...))
}

func PrintlnWithKV(msg, key, value string) {
	LogWithKVs(&KeyValue{
		"msg": msg,
		key:   value,
	})
}

func Fatal(v ...interface{}) {
	Println(v...)
	os.Exit(1)
}

func Fatalf(format string, v ...interface{}) {
	Printf(format, v)
	os.Exit(1)
}

func LogWithKV(key string, value interface{}) {
	log.Println(&KeyValue{key: value})
}

func LogWithKVs(data *KeyValue) {
	log.Println(data)
}
