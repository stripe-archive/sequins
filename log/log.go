package log

import (
	"log"
	"bytes"
	"fmt"
	"strings"
	"os"
)

const SEQUINS_LOG_LINE  = "CANONICAL-SEQUINS-LINE: "

type KeyValue struct {
	Key string
	Value string
}

func Println(msg ...string) {
	LogWithKV("msg", strings.Join(msg, " "))
}

func Printf(format string, v ...interface{}) {
	Println(fmt.Sprintf(format, v...))
}

func PrintlnWithKV(msg, key, value string) {

	LogWithKVs([]*KeyValue{
		&KeyValue{
			Key: "msg",
			Value: msg,
		},
		&KeyValue{
			Key: key,
			Value:value,
		},
	},
	)
}

func Fatal(v ...interface{}) {
	Println(fmt.Sprint(v...))
	os.Exit(1)
}

func FatalF(format string, v ...interface{}) {
	Printf(format, v)
	os.Exit(1)
}
func LogWithKV(key, value string) {
	kv := &KeyValue{
		Key: key,
	Value: value,
	}
	response := []*KeyValue{kv}
	LogWithKVs(response)
}

func LogWithKVs(data []*KeyValue) {
	var buffer bytes.Buffer
	buffer.WriteString(SEQUINS_LOG_LINE)
	for _, kv := range data {
		buffer.WriteString(fmt.Sprintf("%s=%s ", kv.Key, kv.Value))
	}

	log.Printf(buffer.String())
}