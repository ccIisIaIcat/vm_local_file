package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/PuerkitoBio/goquery"
)

func get_html_info() {
	fmt.Println("a")
	res, err := http.Get("https://www.cmegroup.com/market-data/delayed-quotes/metals.html")
	fmt.Println("b")
	if err != nil {
		fmt.Println(err)
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		fmt.Printf("status code error: %d %s", res.StatusCode, res.Status)
	}
	doc, err := goquery.NewDocumentFromReader(res.Body)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(doc)
}

func main() {
	fmt.Println("hello world")
	get_html_info()
}
