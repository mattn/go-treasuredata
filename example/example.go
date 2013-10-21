package main

import (
	"fmt"
	"github.com/mattn/go-treasuredata"
	"log"
	"os"
)

func main() {
	client := treasuredata.NewClient(os.Getenv("TREASURE_DATA_API_KEY"))
	databases, err := client.DatabaseList()
	if err != nil {
		log.Fatal(err)
	}
	for _, database := range databases {
		fmt.Println(database.Name)
		fmt.Println(database.Count)
		fmt.Println(database.CreatedAt.String())

		tables, err := client.TableList(database.Name)
		if err != nil {
			log.Fatal(err)
		}
		for _, table := range tables {
			fmt.Println(table.Id)
			fmt.Println(table.Name)
			for _, col := range table.Schema.Columns() {
				fmt.Println(col.Name, col.Type)
			}
		}
	}

	job, err := client.JobIssueHive("mattn", "select * from unko", 2)
	if err != nil {
		log.Fatal(err)
	}

	// Waiing Job
	/*
	for {
		status, err := client.JobStatus(job.JobId)
		if err != nil {
			log.Fatal(err)
		}
		if status.Status != "running" {
			break
		}
		time.Sleep(1 * time.Second)
	}
	*/

	err = client.JobResult(job.JobId, func(line string) error {
		fmt.Println(line)
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}
