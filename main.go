package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type TaskConfig struct {
	Case      string
	DataRule  []int
	DataFile  string
	TempFile  string
	TransNum  int
	ThreadNum int
}

func ABCParser(path string) []TaskConfig {
	fi, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer fi.Close()

	fd, err := ioutil.ReadAll(fi)
	if err != nil {
		panic(err)
	}

	var config []TaskConfig
	err = json.Unmarshal(fd, &config)
	if err != nil {
		panic(err)
	}

	return config
}

type Transaction struct {
	Sqls []string
}

func NewTransaction(sqls []string) *Transaction {
	nums := len(sqls)
	tran := &Transaction{Sqls: make([]string, nums)}
	for i, s := range sqls {
		tran.Sqls[i] = s
	}

	return tran
}

//func TransactionFactory(cfg TaskConfig) []Transaction {
func TransactionFactory(cfg TaskConfig, taskch chan Transaction) {
	/*
		var trans []Transaction

		text := ParseTemplate(transtemp)
		resultSet := SelectData(datafile, datarule, transnum)

		for _, r := range resultSet {
			tran := NewTransaction(BuildTransaction(text, r))
			trans = append(trans, *tran)
		}
	*/

	//return trans
	for {
		sqls := make([]string, 8)
		sqls[0] = "begin"
		sqls[1] = `insert into test values (2);`
		sqls[2] = `insert into test values (2);`
		sqls[3] = `select * from test limit 1;`
		sqls[4] = `insert into test values (2);`
		sqls[5] = `insert into test values (2);`
		sqls[6] = `select * from test limit 1;`
		sqls[7] = "commit"

		trans := NewTransaction(sqls)
		taskch <- *trans
	}

}

func run(db *sql.DB, ch chan int64, taskch chan Transaction) {
	for {
		t0 := time.Now()
		var count int64
		count = 300
		fmt.Println("before")
		for i := int64(0); i < count; i++ {
			trans := <-taskch
			for _, sql := range trans.Sqls {
				rows, err := db.Query(sql)
				if err != nil {
					fmt.Println(err.Error()) // proper error handling instead of panic in your app
					db.Exec("Rollback")
					break
				}
				rows.Close()
			}
		}
		t1 := time.Now()
		// fmt.Printf("%v\n", string(t1.Sub(t0).Nanoseconds()/10))
		// t, _ := time.ParseDuration(fmt.Sprintf("%dns", t1.Sub(t0).Nanoseconds()/count))
		ch <- t1.Sub(t0).Nanoseconds() / count
		fmt.Println("after")
	}
	close(ch)
}

func OpenDatabase() *sql.DB {
	db, err := sql.Open("mysql", "root@unix(/tmp/mysql.sock)/test")
	if err != nil {
		fmt.Println(err.Error())
		panic(err.Error()) // Just for example purpose. You should use proper error handling instead of panic
	}
	err = db.Ping()
	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}
	//db.Exec("truncate test")

	return db
}

func RunTask(t TaskConfig) {
	db := OpenDatabase()
	defer db.Close()

	count := t.ThreadNum
	var chans = make([]chan int64, count)
	for i := range chans {
		chans[i] = make(chan int64)
	}

	var taskchans = make(chan Transaction)

	for i := range chans {
		go run(db, chans[i], taskchans)
	}

	go TransactionFactory(t, taskchans)

	cases := make([]reflect.SelectCase, len(chans))
	for i, ch := range chans {
		cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch)}
	}

	remaining := len(cases)
	for {
		chosen, value, ok := reflect.Select(cases)
		if !ok {
			// The chosen channel has been closed, so zero out the channel to disable the case
			cases[chosen].Chan = reflect.ValueOf(nil)
			remaining -= 1
			continue
		}

		t, _ := time.ParseDuration(fmt.Sprintf("%dns", value.Int()))
		//ts[chosen] = value.Int()
		fmt.Printf("Read from channel %#v and received %s\n", chans[chosen], t)
		fmt.Printf("time duration: %v\n", t)
	}
}

func main() {
	tasks := ABCParser("abc.json")
	//test task
	//for _, t := range tasks {
	//	fmt.Println(t.TempFile)
	//	fmt.Println(t.DataFile)
	//	fmt.Println(t.DataRule)
	//	fmt.Println(t.TransNum)
	//	fmt.Println(t.ThreadNum)
	//}

	for _, t := range tasks {
		go RunTask(t)
	}

	for {
		time.Sleep(100 * time.Millisecond)
	}
	// go run(db, ch)

	// ts[0] = <-ch
	/*
		for i := range ts {
			t, _ := time.ParseDuration(fmt.Sprintf("%dns", ts[i]))
			fmt.Printf("time duration: %v\n", t)
		}
	*/
	// t, _ := time.ParseDuration(fmt.Sprintf("%dns", ts[0]))
	// fmt.Printf("time duration: %v\n", t)
	fmt.Println("Hello")
}
