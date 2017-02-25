package main

import (
	"database/sql"
	"fmt"
	"reflect"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func run(db *sql.DB, ch chan int64) {
	sqls := make([]string, 8)
	sqls[0] = "begin"
	sqls[1] = `insert into test values (2);`
	sqls[2] = `insert into test values (2);`
	sqls[3] = `select * from test limit 1;`
	sqls[4] = `insert into test values (2);`
	sqls[5] = `insert into test values (2);`
	sqls[6] = `select * from test limit 1;`
	sqls[7] = "commit"
	t0 := time.Now()
	var count int64
	count = 300
	for i := int64(0); i < count; i++ {
		for _, sql := range sqls {
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
	close(ch)
}

func main() {
	db, err := sql.Open("mysql", "root@unix(/tmp/mysql.sock)/test")
	if err != nil {
		fmt.Println(err.Error())
		panic(err.Error()) // Just for example purpose. You should use proper error handling instead of panic
	}
	defer db.Close()
	err = db.Ping()
	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}
	db.Exec("truncate test")
	count := 10
	var chans = make([]chan int64, count)
	for i := range chans {
		chans[i] = make(chan int64)
	}
	ts := make([]int64, count, count)
	// ch := chans[0]
	for i := range chans {
		go run(db, chans[i])
	}
	// go run(db, ch)
	cases := make([]reflect.SelectCase, len(chans))
	for i, ch := range chans {
		cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch)}
	}

	remaining := len(cases)
	for remaining > 0 {
		chosen, value, ok := reflect.Select(cases)
		if !ok {
			// The chosen channel has been closed, so zero out the channel to disable the case
			cases[chosen].Chan = reflect.ValueOf(nil)
			remaining -= 1
			continue
		}

		ts[chosen] = value.Int()
		// fmt.Printf("Read from channel %#v and received %s\n", chans[chosen], value.Int())
	}

	// ts[0] = <-ch
	for i := range ts {
		t, _ := time.ParseDuration(fmt.Sprintf("%dns", ts[i]))
		fmt.Printf("time duration: %v\n", t)
	}
	// t, _ := time.ParseDuration(fmt.Sprintf("%dns", ts[0]))
	// fmt.Printf("time duration: %v\n", t)
	fmt.Println("Hello")
}
