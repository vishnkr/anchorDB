use core::str;
use std::io::{self, Write};

use anyhow::{self, Context};
use anchor_db::AnchorDB;


fn main() -> anyhow::Result<()> {
    let mut db = AnchorDB::new();

    println!("Welcome to AnchorDB REPL!");
    println!("Enter 'get <key>', 'put <key> <value>', 'delete <key>', or 'exit' to quit.");

    loop {
        print!("> ");
        io::stdout().flush().expect("Failed to flush stdout");

        let mut input = String::new();
        io::stdin().read_line(&mut input).expect("Failed to read line");
        let input = input.trim();

        // Find the command by splitting at the first whitespace
        let mut parts = input.splitn(2, ' ');
        let command = match parts.next() {
            Some(cmd) => cmd,
            None => continue,
        };

        match command {
            "get" => {
                if let Some(key) = parts.next() {
                    let key_bytes = key.as_bytes();
                    match db.get(key_bytes) {
                        Some(entry) => {
                            let value = str::from_utf8(&entry.value()).unwrap();
                            println!(
                                "Got value: {:?} for key: {:?}, Timestamp: {}",
                                value,
                                key,
                                entry.timestamp()
                            );
                        }
                        None => println!("Cannot get value for key: {:?}", key),
                    }
                } else {
                    println!("Missing key for get command.");
                }
            }
            "put" => {
                if let Some(rest) = parts.next() {
                    // Find the key and value by splitting at the first whitespace
                    let mut kv_parts = rest.splitn(2, ' ');
                    let key = kv_parts.next();
                    let value = kv_parts.next();

                    if let (Some(key), Some(value)) = (key, value) {
                        let key_bytes = key.as_bytes();
                        let value_bytes = value.as_bytes();
                        match db.put(key_bytes, value_bytes) {
                            Ok(_) => println!("Successfully inserted key-value pair."),
                            Err(_) => println!("Failed to insert key-value pair."),
                        }
                    } else {
                        println!("Missing key or value for put command.");
                    }
                } else {
                    println!("Missing key and value for put command.");
                }
            }
            "delete" => {
                if let Some(key) = parts.next() {
                    // Handle delete operation
                    println!("Delete operation not implemented yet.");
                } else {
                    println!("Missing key for delete command.");
                }
            }
            "exit" => {
                println!("Exiting REPL...");
                break;
            }
            _ => {
                println!("Invalid command. Try 'get <key>', 'put <key> <value>', 'delete <key>', or 'exit'.");
            }
        }
    }
    Ok(())
}