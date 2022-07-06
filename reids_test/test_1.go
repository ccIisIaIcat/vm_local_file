package main

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/PuerkitoBio/goquery"
)

func main() {
	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	req, _ := http.NewRequest("GET", "https://www.cmegroup.com/market-data/delayed-quotes/metals.html", nil)
	req.Header.Set("User-Agent", "PostmanRuntime/7.29.0")
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("%s\n", err.Error())
		return
	}
	defer resp.Body.Close()
	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		log.Fatal(err)
	}
	doc.Find(".cmeTableLeft").Each(func(i int, selection *goquery.Selection) {
		fmt.Println(selection.Text())
		fmt.Println("lala")
	})
}
