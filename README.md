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
  "github.com/fluent/fluent-logger-golang/fluent"
  "fmt"
  "time"
)

func main() {
  logger, err := fluent.New(fluent.Config{}, nil)
  if err != nil {
    fmt.Println(err)
  }
  defer logger.Close()
  tag := "myapp.mega"
  var data = map[string]string{
    "foo":  "bar",
    "hoge": "hoge",
  }
  error := logger.Push(tag, data)
  if error != nil {
    panic(error)
  }
}
```
