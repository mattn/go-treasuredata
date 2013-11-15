# go-treasuredata

Go interface to [Treasure Data](http://www.treasure-data.com/)

## Installation

    $ go get github.com/mattn/go-treasuredata

## Usage

```go
client := treasuredata.NewClient(env)
job, _ := client.JobIssueHive("mydb", "select * from mytable")
client.JobResultFunc(job.JobId, func(row []interface{}) error {
	fmt.Println(row)
	return nil
})
```

## License

MIT

## Author

Yasuhiro Matsumoto (a.k.a mattn)
