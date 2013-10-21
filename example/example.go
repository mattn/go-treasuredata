package main

import (
	"flag"
	"fmt"
	"github.com/mattn/go-treasuredata"
	"log"
	"os"
)

var info = flag.Bool("i", false, "information of databases")
var db = flag.String("d", "", "database")
var query = flag.String("q", "", "query string")

func main() {
	flag.Parse()

	client := treasuredata.NewClient(os.Getenv("TREASURE_DATA_API_KEY"))
	databases, err := client.DatabaseList()
	if err != nil {
		log.Fatal(err)
	}

	if *info || *db == "" || *query == "" {
		for _, database := range databases {
			fmt.Println("DATABASE:", database.Name)
			fmt.Println("  Record Count:", database.Count)
			fmt.Println("  Created At:", database.CreatedAt.String())

			tables, err := client.TableList(database.Name)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println()
			for _, table := range tables {
				fmt.Println("  TABLE:", table.Id, table.Name)
				for _, col := range table.Schema.Columns() {
					fmt.Println("    COLUMN:", col.Name, col.Type)
				}
			}
		}
	} else {
		job, err := client.JobIssueHive(*db, *query)
		if err != nil {
			log.Fatal(err)
		}

		err = client.JobResult(job.JobId, func(line string) error {
			fmt.Println(line)
			return nil
		})
		if err != nil {
			log.Fatal(err)
		}
	}
}
