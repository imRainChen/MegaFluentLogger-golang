package main

import (
	"fmt"
	"mega-fluent-logger-golang/fluentd"
)

func main() {
	logger, err := fluentd.New(fluentd.Config{}, fluentd.NewLogger())
	if err != nil {
		fmt.Println(err)
	}
	defer logger.Close()

	tag := "app.mega"
	var data = map[string]string{
		"foo":  "bar",
		"hoge": "hoge",
	}

	err = logger.Push(tag, data)
	if err != nil {
		panic(err)
	}
}
