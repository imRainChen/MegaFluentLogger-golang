安装
----------
```
go get github.com/imRainChen/mega-fluent-logger-golang/fluentd
```

功能特性
----------
 - 异步无阻塞发送
 - 底层批量发送日志
 - ACK确认
 - 文件队列缓存

Example
----------
```golang
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

	tag := "game.mega"
	var data = map[string]string{
		"foo":  "bar",
		"hoge": "hoge",
	}

	err = logger.Push(tag, data)
	if err != nil {
		panic(err)
	}
}

```
