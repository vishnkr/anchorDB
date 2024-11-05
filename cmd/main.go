package main

import (
	anchordb "anchordb"
	"bufio"
	"fmt"
	"os"
	"strings"
)

func main(){
	/*db,err := anchordb.Open("./")
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
	db.Get("hello1s")*/
	rootCmd := anchordb.InitializeCLI() 
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("AnchorDB REPL (type 'exit' to quit)")
	for {
		fmt.Print(">>> ")
		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading input:", err)
			continue
		}

		input = strings.TrimSpace(input)
		if input == "exit" {
			fmt.Println("Exiting AnchorDB REPL.")
			break
		}

		// Split input into args for Cobra command parsing
		args := strings.Fields(input)
		if len(args) == 0 {
			continue
		}

		// Execute the CLI command
		rootCmd.SetArgs(args)
		if err := rootCmd.Execute(); err != nil {
			fmt.Println("Command error:", err)
		}
	}

}