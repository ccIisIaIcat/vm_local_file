package main

import (
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/garyburd/redigo/redis"
)

type Info_struct struct {
	//用于建立redis连接
	redis_connection redis.Conn
	//主力合约键
	MAIN_FORCE string
	//黄金
	MAINGOLD string //黄金主力合约
	WGJS     string //国际黄金
	SGE      string //上海黄金交易所
	gold     string //实物黄金
	//股指债券
	GJZS      string //全球股指
	SHARESASI string //亚洲股指
	SHARESEUR string //欧洲股指
	SHARESAME string //美洲股指
	GJZQ      string //国际债券
	//原油
	MAINOIL string //原油主力合约
	INE     string //上海能源
	NYMEX   string //纽约MEX
	//外盘
	COMEX string //纽约COMEX

}

func (I *Info_struct) init() {
	var err error
	I.redis_connection, err = redis.Dial("tcp", "127.0.0.1:6379")
	if err != nil {
		fmt.Println("Connect to redis error", err)
		return
	}

	I.MAINGOLD = "https://quote.fx678.com/exchange/MAINGOLD"
	I.WGJS = "https://quote.fx678.com/exchange/WGJS"
	I.SGE = "https://quote.fx678.com/exchange/SGE"
	I.gold = "https://quote.fx678.com/gold"
	I.GJZS = "https://quote.fx678.com/exchange/GJZS"
	I.SHARESASI = "https://quote.fx678.com/exchange/SHARESASI"
	I.SHARESEUR = "https://quote.fx678.com/exchange/SHARESEUR"
	I.SHARESAME = "https://quote.fx678.com/exchange/SHARESAME"
	I.GJZS = "https://quote.fx678.com/exchange/GJZQ"
	I.COMEX = "https://quote.fx678.com/exchange/COMEX"
	I.MAINOIL = "https://quote.fx678.com/exchange/MAINOIL"
	I.INE = "https://quote.fx678.com/exchange/INE"

}

// 用于返回对应种类的字典
func (I *Info_struct) Search(info_type string) map[string](map[string]string) {
	res, err := http.Get(info_type)
	if err != nil {
		log.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		log.Fatalf("status code error: %d %s", res.StatusCode, res.Status)
	}

	doc, err := goquery.NewDocumentFromReader(res.Body)
	if err != nil {
		log.Fatal(err)
	}
	tool_list := make([][]string, 0)
	doc.Find("tr").Each(func(i int, selection *goquery.Selection) {
		b := strings.Replace(selection.Text(), " ", "", -1)
		dede := strings.Split(b, "\n")
		tool_list = append(tool_list, dede)
	})
	tool_list[0] = append(tool_list[0][1:2], tool_list[0][5:]...)
	tool_list[0] = tool_list[0][:len(tool_list[0])-1]
	name_list := tool_list[0]
	tool_map := make(map[string](map[string]string), 0)
	for i := 1; i < len(tool_list)-1; i++ {
		for j := 0; j < len(tool_list[i]); j++ {
			if tool_list[i][j] == "" || tool_list[i][j] == " " {
				tool_list[i] = append(tool_list[i][:j], tool_list[i][j+1:]...)
			}
		}
		tool_list[i] = tool_list[i][1:]
		tool_list[i] = append(tool_list[i][:1], tool_list[i][2:]...)
		tool_map[tool_list[i][0]] = make(map[string]string, 0)
		for j := 1; j < len(name_list); j++ {
			(tool_map[tool_list[i][0]])[name_list[j]] = tool_list[i][j]
		}

	}
	return tool_map
}

//用于判断两个表是否有变化
func judge(m1 map[string](map[string]string), m2 map[string](map[string]string)) bool {
	answer := false
	for k := range m1 {
		for k_ := range m1[k] {
			if m2[k][k_] != m1[k][k_] {
				answer = true
			}
		}
	}
	return answer
}

//用于存储与redis
func (I *Info_struct) save_in_redis(info_map map[string](map[string]string)) {
	for k := range info_map {
		for k_, v_ := range info_map[k] {
			_, err := I.redis_connection.Do("hset", k, k_, v_)
			if err != nil {
				fmt.Println("err", err)
			}
		}
	}

	for k := range info_map {
		for k_ := range info_map[k] {
			ans, err := I.redis_connection.Do("hget", k, k_)
			if err != nil {
				fmt.Println("err", err)
			} else {
				fmt.Println(k, " ", k_, " ", string(ans.([]uint8)))
			}
		}
	}

}

func main() {
	i := Info_struct{}
	i.init()
	tool_0 := i.Search(i.COMEX)
	i.save_in_redis(tool_0)
	a := 0
	for {
		a += 1
		//每0.5秒轮询一次，有变化就输出结果
		ans, _ := i.redis_connection.Do("hget", "纽约白银连续", "最新价")
		fmt.Println(a)
		fmt.Println("纽约白银09", "最新价", string(ans.([]uint8)))
		time.Sleep(time.Millisecond * 500)
		tool_1 := i.Search(i.COMEX)
		if judge(tool_0, tool_1) {
			tool_0 = tool_1
			i.save_in_redis(tool_0)
		}
		if a == 30 {
			break
		}
	}
	i.redis_connection.Do("flushall")
}
