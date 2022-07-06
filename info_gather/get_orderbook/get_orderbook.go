package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/websocket"
)

//订单簿结构体
type orderbook struct {
	ts        int
	checksum  float64
	asks_list map[string]([]string)
	bids_list map[string]([]string)
}

//信息传递结构体
type info_sender struct {
	Op   string        `json:"op"`
	Args []args_struct `json:"args"`
}
type args_struct struct {
	Channel string `json:"channel"`
	InstID  string `json:"instId"`
}

type Orderbook_info struct {
	logger *log.Logger //用于输出日志文件

	websocket_address string
	connect           *websocket.Conn //websocket连接

	infogather_original chan []byte //用于存放数据的channal
	//中间在开通两个新的通道，将infogather_origianl分为infogather_orderbook和infogather_trades
	//用于分别储存订单簿和成交信息
	infogather_orderbook chan []byte    //用于分流订单簿信息
	infogather_trades    chan []byte    //用于分流成交信息
	processed_data       chan orderbook //用于存放处理完的数据的channel

	signal_get_original_data      bool //用于结束infogather_original对应的进程
	signal_get_processed_data     bool //用于结束processed_data对应的进程
	signal_insert_trade_table     bool //用于结束nsert_trade_table的录入
	signal_insert_orderbook_table bool //用于结束nsert_orderbook_table的录入

	signal_logger bool //用与标的是否将过程输出显示并写入日志

	db *sql.DB //mysql数据库指针
}

func (O *Orderbook_info) init() {

	//把日志文件存在info_gather的日志文件夹下
	file := "../log_files/" + time.Now().Format("2006-01-02") + ".txt"
	logFile, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0766)
	if err != nil {
		panic(err)
	}
	O.logger = log.New(logFile, "[okex_websocket]", log.Lshortfile|log.Lmicroseconds) // 将文件设置为loger作为输出
	//初始化okex的websocket地址
	O.websocket_address = "wss://ws.okx.com:8443/ws/v5/public"
	//初始化用于存放原始信息的chan
	O.infogather_original = make(chan []byte, 100)
	//初始化用于分流的两股信息的chan
	O.infogather_orderbook = make(chan []byte, 100)
	O.infogather_trades = make(chan []byte, 100)
	//初始化用于存放处理后信息的chan
	O.processed_data = make(chan orderbook, 100)
	//初始化进程标记
	O.signal_get_original_data = true
	O.signal_get_processed_data = true
	O.signal_insert_trade_table = true
	O.signal_insert_orderbook_table = true

	O.signal_logger = true
	//方便起见直接声明一个用于存放订单簿和交易数据的db
	dsn := "root" + ":" + "" + "@tcp(127.0.0.1:3306)/" + "orderbook_and_trade"
	O.db, err = sql.Open("mysql", dsn) //defer db.Close() // 注意这行代码要写在上面err判断的下面
	if err != nil {
		fmt.Println("mysql建立链接出错:", err)
		return
	}
	err = O.db.Ping()
	if err != nil {
		fmt.Println("mysql建立链接出错:")
		panic(err)
	}
	fmt.Println("mysql连接成功!")

}

//创建一个数据库，用于存放交易信息，包括【时间戳——timestamp，bigint;存入时的时间戳——ts_save,bigint;交易价格——price，double；成交量——size，double；买卖方——side，int】
//可以尝试加入加入本地时间戳，添加本地的一个时间戳
func (O *Orderbook_info) make_trade_table() {
	sql := "CREATE TABLE " + "trade_info" + "(id int PRIMARY KEY AUTO_INCREMENT,timestamp bigint,ts_save bigint,price double,size double,side int)" + "ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;"
	_, err := O.db.Exec(sql)
	if err != nil {
		O.logger.Panic("发生错误:", err)
	} else {
		O.logger.Println("trade_info建表成功")
	}
}

//编写一个函数用于把trade信息存入trade_table中
func (O *Orderbook_info) insert_trade_table() {
	sql_insert := "insert into trade_info (timestamp,ts_save,price,size,side) values(?,?,?,?,?)"
	stmt, err := O.db.Prepare(sql_insert)
	if err != nil {
		O.logger.Panic("stmt错误:", err)
	}
	defer stmt.Close()
	var result map[string]interface{}
	for O.signal_insert_trade_table {
		data := <-O.infogather_trades
		json.Unmarshal(data, &result)
		data_2 := (result["data"].([]interface{})[0]).(map[string]interface{})
		time_stamp, _ := strconv.Atoi(data_2["ts"].(string))
		price, _ := strconv.ParseFloat(data_2["px"].(string), 64)
		size, _ := strconv.ParseFloat(data_2["sz"].(string), 64)
		side := 0
		if data_2["side"].(string) == "sell" {
			side = 1
		}
		stmt.Exec(time_stamp, time.Now().UnixNano()/10e5, price, size, side)
	}

}

//创建一个数据库，用于存放订单簿信息，包括【时间戳——timestamp bigint;存入时的时间戳——ts_save bigint;校验和-checksum int;卖方订单簿——asks_list json;买方订单簿——bids_list json】
func (O *Orderbook_info) make_orderbook_table(orderbook_type string) {
	sql := "CREATE TABLE " + "orderbook_info" + "(id int PRIMARY KEY AUTO_INCREMENT,timestamp bigint,ts_save bigint,checksum double,asks_list BLOB,bids_list BLOB)" + "ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;"
	if orderbook_type == "books5" {
		sql = "CREATE TABLE " + "orderbook_info" + "(id int PRIMARY KEY AUTO_INCREMENT,timestamp bigint,ts_save bigint,checksum double,asks_list tinyBLOB,bids_list tinyBLOB)" + "ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;"
	}
	_, err := O.db.Exec(sql)
	if err != nil {
		O.logger.Panic("发生错误:", err)
	} else {
		O.logger.Println("orderbook_info建表成功")
	}
}

//编写一个函数用于把trade信息存入trade_table中
func (O *Orderbook_info) insert_orderbook_table() {
	sql_insert := "insert into orderbook_info (timestamp,ts_save,checksum,asks_list,bids_list) values(?,?,?,?,?)"
	stmt, err := O.db.Prepare(sql_insert)
	if err != nil {
		O.logger.Panic("stmt错误:", err)
	}
	defer stmt.Close()
	for O.signal_insert_orderbook_table {
		data := <-O.processed_data
		tool_asks, _ := json.Marshal(data.asks_list)
		tool_bids, _ := json.Marshal(data.bids_list)
		stmt.Exec(data.ts, time.Now().UnixNano()/10e5, data.checksum, tool_asks, tool_bids)

	}

}

//建立websocket连接
func (O *Orderbook_info) start_websocket() {
	dialer := websocket.Dialer{}
	var err error
	O.connect, _, err = dialer.Dial(O.websocket_address, nil)
	if nil != err {
		O.logger.Println(err)
		return
	}
	O.logger.Println("websocket连接已建立")
}

//结束websocket连接
func (O *Orderbook_info) end_websocket() {
	O.connect.Close()
	O.logger.Println("websocket连接已断开")
}

//向websocket订阅或取消订阅一个服务
//channel_info为订阅的频道的字符串，instrument_id为币对的字符串，subscribe为确定是订阅还是取消订阅的布尔值
func (O *Orderbook_info) control_a_server(channel_info string, instrument_id string, subscribe bool) {
	i_f_subscribe := info_sender{}
	if subscribe {
		i_f_subscribe = info_sender{Op: "subscribe", Args: []args_struct{{Channel: channel_info, InstID: instrument_id}}}
	} else {
		i_f_subscribe = info_sender{Op: "unsubscribe", Args: []args_struct{{Channel: channel_info, InstID: instrument_id}}}
	}
	message_send, err := json.Marshal(i_f_subscribe)
	if err != nil {
		O.logger.Println("转为json文件失败:", err)
	}
	err = O.connect.WriteMessage(websocket.TextMessage, message_send)
	if err != nil {
		O.logger.Println("订阅失败", err)
	} else {
		if subscribe {
			O.logger.Println("订阅已申请,订阅json:", string(message_send))
		} else {
			O.logger.Println("订阅已取消,订阅json:", string(message_send))
		}
	}
	//每申请一个服务，对应生成一个通道，和一个wg，然后开启对应的save_in_channel，函数返回对饮channel
	//每
}

//将收到的信息存放于特定channel中
//更新：设置一个十秒的定时器，如果十秒内没有收到消息，则发送ping，如果没有收到pong，则断线重连
func (O *Orderbook_info) save_in_channel(orderbook_type string, trades_type string, instrument_id string) {
	for O.signal_get_original_data {
		signal_1 := false
		signal_2 := false
		go func() {
			for i := 0; i < 20; i++ {
				time.Sleep(time.Second * 1)
				if signal_1 {
					return
				}
			}
			O.logger.Println("websocket二十秒内无反应,发送ping值校验")
			O.connect.WriteMessage(websocket.PingMessage, []byte{})
			go func() {
				for i := 0; i < 15; i++ {
					time.Sleep(time.Second * 1)
					if signal_2 {
						return
					}
				}
				O.logger.Println("未收到返回pong值,正尝试重启websocket服务")
				O.restart(24*60, orderbook_type, instrument_id)
			}()
			messageType, messageData, _ := O.connect.ReadMessage()
			if messageType == websocket.PongMessage {
				O.logger.Println("收到pong值:", messageData)
			}
			signal_2 = true
		}()
		messageType, messageData, err := O.connect.ReadMessage()
		signal_1 = true
		if err != nil {
			O.logger.Println("websocket_get_err:", err)
			O.logger.Println("websocket可能已经断开,直接重启websocket吧,只能先这样了")
			O.restart(24*60, orderbook_type, instrument_id)
		}
		switch messageType {
		case websocket.TextMessage: //文本数据
			temp_data := messageData
			var result map[string]interface{}
			json.Unmarshal(temp_data, &result)
			if result["arg"] != nil {
				temp_data_2 := result["arg"].(map[string]interface{})
				if result["data"] != nil {
					switch temp_data_2["channel"].(string) {
					case orderbook_type:
						O.infogather_orderbook <- messageData
					case trades_type:
						O.infogather_trades <- messageData
					}
				}
			}
		case websocket.BinaryMessage: //二进制数据
			O.infogather_original <- messageData
		case websocket.CloseMessage: //关闭
		case websocket.PingMessage: //Ping
			O.connect.WriteMessage(websocket.PongMessage, messageData)
			O.logger.Println("回复pong值:", err)
		case websocket.PongMessage: //Pong
		default:
			// O.logger.Println("unkown_message_type:", messageType, "data:", messageData)
		}
	}
}

// 用于提取原始channel中的数据，并在加工处理后存放在新的数据channel中
func (O *Orderbook_info) put_orderbook_into_channel() {
	var result map[string]interface{}
	my_orderbook := orderbook{}
	for O.signal_get_processed_data {
		data := <-O.infogather_orderbook
		json.Unmarshal(data, &result)
		if result["action"] == "snapshot" {
			new_data := (result["data"].([]interface{}))[0].(map[string]interface{})
			ts := new_data["ts"]
			checksum := new_data["checksum"]
			asks_list_interface := new_data["asks"].([]interface{})
			bids_list_interface := new_data["bids"].([]interface{})
			asks_list := make(map[string]([]string), 0)
			bids_list := make(map[string]([]string), 0)
			for i := 0; i < len(asks_list_interface); i++ {
				temp_data := asks_list_interface[i].([]interface{})
				asks_list[temp_data[0].(string)] = []string{temp_data[1].(string), temp_data[3].(string)}
			}
			for i := 0; i < len(bids_list_interface); i++ {
				temp_data := bids_list_interface[i].([]interface{})
				bids_list[temp_data[0].(string)] = []string{temp_data[1].(string), temp_data[3].(string)}
			}
			n_ts, _ := strconv.Atoi(ts.(string))
			my_orderbook.ts, my_orderbook.asks_list, my_orderbook.bids_list, my_orderbook.checksum = n_ts, asks_list, bids_list, checksum.(float64)
			temp_new_orderbook := my_orderbook
			O.processed_data <- temp_new_orderbook
		} else if result["action"] == "update" {
			new_data := (result["data"].([]interface{}))[0].(map[string]interface{})
			ts := new_data["ts"]
			n_ts, _ := strconv.Atoi(ts.(string))
			checksum := new_data["checksum"]
			my_orderbook.ts, my_orderbook.checksum = n_ts, checksum.(float64)
			asks_list_interface := new_data["asks"].([]interface{})
			bids_list_interface := new_data["bids"].([]interface{})
			for i := 0; i < len(asks_list_interface); i++ {
				temp_data := asks_list_interface[i].([]interface{})
				if temp_data[1].(string) == "0" {
					if _, ok := (my_orderbook.asks_list)[temp_data[0].(string)]; ok {
						delete(my_orderbook.asks_list, temp_data[0].(string))
					}
				} else {
					my_orderbook.asks_list[temp_data[0].(string)] = []string{temp_data[1].(string), temp_data[3].(string)}
				}
			}
			for i := 0; i < len(bids_list_interface); i++ {
				temp_data := bids_list_interface[i].([]interface{})
				if temp_data[1].(string) == "0" {
					if _, ok := (my_orderbook.bids_list)[temp_data[0].(string)]; ok {
						delete(my_orderbook.bids_list, temp_data[0].(string))
					}
				} else {
					my_orderbook.bids_list[temp_data[0].(string)] = []string{temp_data[1].(string), temp_data[3].(string)}
				}
			}
			temp_new_orderbook := my_orderbook
			O.processed_data <- temp_new_orderbook
		} else if result["data"] != nil {
			new_data := (result["data"].([]interface{}))[0].(map[string]interface{})
			ts := new_data["ts"]
			asks_list_interface := new_data["asks"].([]interface{})
			bids_list_interface := new_data["bids"].([]interface{})
			asks_list := make(map[string]([]string), 0)
			bids_list := make(map[string]([]string), 0)
			for i := 0; i < len(asks_list_interface); i++ {
				temp_data := asks_list_interface[i].([]interface{})
				asks_list[temp_data[0].(string)] = []string{temp_data[1].(string), temp_data[3].(string)}
			}
			for i := 0; i < len(bids_list_interface); i++ {
				temp_data := bids_list_interface[i].([]interface{})
				bids_list[temp_data[0].(string)] = []string{temp_data[1].(string), temp_data[3].(string)}
			}
			n_ts, _ := strconv.Atoi(ts.(string))
			my_orderbook.ts, my_orderbook.asks_list, my_orderbook.bids_list, my_orderbook.checksum = n_ts, asks_list, bids_list, 0
			temp_new_orderbook := my_orderbook
			O.processed_data <- temp_new_orderbook
		}
	}
}

//停止各个进程
func (O *Orderbook_info) stop_channel() {
	O.signal_get_original_data = false
	O.signal_get_processed_data = false
	O.signal_insert_trade_table = false
	O.signal_insert_orderbook_table = false
}

func (O *Orderbook_info) start_server(orderbook_type string, trade_type string, instrument_ID string) {
	O.init()
	O.start_websocket()
	O.make_trade_table()
	O.make_orderbook_table(orderbook_type)
	O.control_a_server(orderbook_type, instrument_ID, true)
	O.control_a_server(trade_type, instrument_ID, true)
}
func (O *Orderbook_info) start_server_2(orderbook_type string, trade_type string, instrument_ID string) {
	O.init()
	O.signal_logger = false
	O.start_websocket()
	O.control_a_server(orderbook_type, instrument_ID, true)
	O.control_a_server(trade_type, instrument_ID, true)
}

func (O *Orderbook_info) process_and_save(orderbook_type string, instrument_id string) {
	go O.save_in_channel(orderbook_type, "trades", instrument_id)
	go O.put_orderbook_into_channel()
	go O.insert_trade_table()
	go O.insert_orderbook_table()

}

func (O *Orderbook_info) end_server(orderbook_type string) {
	O.stop_channel()
	time.Sleep(time.Second)
	O.control_a_server(orderbook_type, "BTC-USDT", false)
	O.control_a_server("trades", "BTC-USDT", false)
	O.end_websocket()
}

func (O *Orderbook_info) restart(time_minutes int, orderbook_type string, instrument_id string) {
	O.stop_channel()
	Start_2(time_minutes, orderbook_type, instrument_id)

}

func Start_(time_minutes int, orderbook_type string, instrument_id string) {
	o_i := Orderbook_info{}
	o_i.start_server(orderbook_type, "trades", instrument_id)
	go o_i.process_and_save(orderbook_type, instrument_id)
	for i := 0; i < time_minutes; i++ {
		time.Sleep(time.Second * 60)
		if o_i.signal_logger {
			fmt.Println("服务已获取信息 ", i+1, " 分钟,总任务时间 ", time_minutes, "分钟,完成比例: ", float64(i+1)/float64(time_minutes))
			if i%59 == 0 {
				o_i.logger.Println("服务已运行", i/int(59), "小时,未发生异常")
			}
		}
	}
	o_i.end_server(orderbook_type)

}
func Start_2(time_minutes int, orderbook_type string, instrument_id string) {
	o_i := Orderbook_info{}
	o_i.start_server_2(orderbook_type, "trades", instrument_id)
	go o_i.process_and_save(orderbook_type, instrument_id)
	for i := 0; i < time_minutes; i++ {
		time.Sleep(time.Second * 60)
		if o_i.signal_logger {
			fmt.Println("服务已获取信息 ", i+1, " 分钟,总任务时间 ", time_minutes, "分钟,完成比例: ", float64(i+1)/float64(time_minutes))
			if i%59 == 0 {
				o_i.logger.Println("服务已运行", i/int(59), "小时,未发生异常")
			}
		}

	}
	o_i.end_server(orderbook_type)

}

func main() {
	//设定录入时间
	time_length := 24 * 60
	Start_(time_length, "books5", "ETH-USDT")
}
