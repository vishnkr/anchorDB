package main

import (
	anchordb "anchor-db"
	"fmt"
)

func main(){
	db,err := anchordb.Open("./")
	if err!=nil{
		return
	}
	db.Put("hello",[]byte("world1323"))
	db.Put("hello1",[]byte("wos"))
	db.Put("hello2",[]byte("wo1323"))
	db.Put("hello3",[]byte("world3"))
	fmt.Printf("%s, %s, %s, %s\n",db.Get("hello"),db.Get("hello1"),db.Get("hello2"),db.Get("hello3"))
	db.Delete("hello2")
	db.Delete("hello3")
	fmt.Printf("%s, %s, %s\n",db.Get("hello"),db.Get("hello2"),db.Get("hello3"))
	db.Get("hello1s")
}