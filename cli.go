package anchordb

import (
	"fmt"
	"log"

	"github.com/spf13/cobra"
)

var db *AnchorDB

func InitializeCLI() *cobra.Command {
	var err error
			db, err = Open("data",nil)
			if err != nil {
				log.Fatalf("Failed to open database: %v", err)
			}
	rootCmd := &cobra.Command{
		Use:   "anchordb",
		Short: "CLI for AnchorDB, a simple embedded key-value store",
		PersistentPostRun: func(cmd *cobra.Command, args []string) {
		},
	}

	rootCmd.AddCommand(putCmd, getCmd, deleteCmd)
	return rootCmd
}

var putCmd = &cobra.Command{
	Use:   "put [key] [value]",
	Short: "Store a key-value pair in the database",
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		key, value := args[0], []byte(args[1])
		db.Put([]byte(key), value)
		fmt.Printf("Stored key=%s, value=%s\n", key, value)
	},
}

var getCmd = &cobra.Command{
	Use:   "get [key]",
	Short: "Retrieve the value for a given key",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		key := args[0]
		value,err := db.Get([]byte(key))
		if err != nil {
			fmt.Printf("Key %s does not exist\n",key)
		} else {
			fmt.Printf("Retrieved key=%s, value=%s\n", key, value)
		}
	},
}

var deleteCmd = &cobra.Command{
	Use:   "delete [key]",
	Short: "Delete a key-value pair from the database",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		key := args[0]
		db.Delete(key)
		fmt.Printf("Deleted key=%s\n", key)
	},
}