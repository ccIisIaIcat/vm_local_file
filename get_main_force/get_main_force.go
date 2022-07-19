package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
)

func get_html_info() {
	res, err := http.Get("http://127.0.0.1:8898")
	if err != nil {
		fmt.Println(err)
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		fmt.Printf("status code error: %d %s", res.StatusCode, res.Status)
	}
	s, _ := ioutil.ReadAll(res.Body) //把	body 内容读入字符串 s
	fmt.Println(string(s))
}

func main() {
	fmt.Println("hello world")
	get_html_info()
}
