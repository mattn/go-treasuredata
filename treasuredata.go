package treasuredata

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const EndPoint = "https://api.treasure-data.com"

type client struct {
	apikey string
	*http.Client
	Debug bool
}

func tee(r io.Reader, debug bool) io.Reader {
	if !debug {
		return r
	}
	return io.TeeReader(r, os.Stdout)
}

var timeFormats = []string{
	"2006-01-02 15:04:05 UTC",
	"2006-01-02T15:04:05Z",
}

type tdTime time.Time

func (tdt tdTime) ToTime() time.Time {
	return time.Time(tdt)
}

func (tdt tdTime) String() string {
	return time.Time(tdt).String()
}

func (tdt *tdTime) UnmarshalJSON(data []byte) error {
	var s string
	var err error

	if len(data) < 2 || (len(data) == 2 && string(data) == `""`) {
		return nil
	}
	if len(data) == 4 && string(data) == `null` {
		return nil
	}

	b := bytes.NewBuffer(data)
	if err := json.NewDecoder(b).Decode(&s); err != nil {
		return err
	}
	var t time.Time
	for _, timeFormat := range timeFormats {
		t, err = time.Parse(timeFormat, s)
		if err == nil {
			break
		}
	}
	if err != nil {
		return err
	}
	*tdt = tdTime(t)
	return nil
}

func NewClient(apikey string) *client {
	return &client{apikey, http.DefaultClient, false}
}

type Database struct {
	Name      string `json:"name"`
	Count     int64  `json:"count"`
	CreatedAt tdTime `json:"created_at"`
}

func (c *client) get(path string) (*http.Response, error) {
	req, err := http.NewRequest("GET", EndPoint+path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "TD1 "+c.apikey)
	return c.Do(req)
}

func (c *client) post(path string, values url.Values) (*http.Response, error) {
	req, err := http.NewRequest("POST", EndPoint+path, strings.NewReader(values.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "TD1 "+c.apikey)
	return c.Do(req)
}

func (c *client) DatabaseList() ([]Database, error) {
	res, err := c.get("/v3/database/list")
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	var r struct {
		Databases []Database `json:"databases"`
	}
	err = json.NewDecoder(tee(res.Body, c.Debug)).Decode(&r)
	if err != nil {
		return nil, err
	}
	return r.Databases, nil
}

type tdSchema string

type Column struct {
	Name string
	Type string
}

func (tds tdSchema) Columns() []Column {
	cols := make([]Column, 0)
	var cells [][]string
	err := json.Unmarshal([]byte(tds), &cells)
	if err == nil {
		for _, cell := range cells {
			cols = append(cols, Column{cell[0], cell[1]})
		}
	}
	return cols
}

type Table struct {
	Id                   int64    `json:"id"`
	Name                 string   `json:"name"`
	Schema               tdSchema `json:"schema"`
	EstimatedStorageSize int64    `json:"estimated_storage_size"`
	CounterUpdatedAt     tdTime   `json:"counter_updated_at"`
	Type                 string   `json:"type"`
	Count                int64    `json:"count"`
	CreatedAt            tdTime   `json:"created_at"`
	UpdatedAt            tdTime   `json:"updated_at"`
}

func (c *client) TableList(database string) ([]Table, error) {
	res, err := c.get("/v3/table/list/" + url.QueryEscape(database))
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	var r struct {
		Tables []Table `json:"tables"`
	}
	err = json.NewDecoder(tee(res.Body, c.Debug)).Decode(&r)
	if err != nil {
		return nil, err
	}
	return r.Tables, nil
}

type Job struct {
	JobId string `json:"job_id"`
}

func (c *client) JobIssueHive(database string, query string) (*Job, error) {
	params := make(url.Values)
	params.Set("query", query)
	res, err := c.post("/v3/job/issue/hive/"+url.QueryEscape(database), params)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	var r Job
	err = json.NewDecoder(tee(res.Body, c.Debug)).Decode(&r)
	if err != nil {
		return nil, err
	}
	return &r, nil
}

func (c *client) JobIssueHiveWithPriority(database string, query string, priority int) (*Job, error) {
	params := make(url.Values)
	params.Set("query", query)
	params.Set("priority", fmt.Sprint(priority))
	res, err := c.post("/v3/job/issue/hive/"+url.QueryEscape(database), params)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	var r Job
	err = json.NewDecoder(tee(res.Body, c.Debug)).Decode(&r)
	if err != nil {
		return nil, err
	}
	return &r, nil
}

type Status struct {
	JobId     string  `json:"job_id"`
	Status    string  `json:"status"`
	CreatedAt tdTime  `json:"created_at"`
	UpdatedAt tdTime  `json:"updated_at"`
	StartAt   tdTime  `json:"start_at"`
	EndAt     *tdTime `json:"end_at"`
}

func (c *client) JobStatus(jobId string) (*Status, error) {
	res, err := c.get("/v3/job/status/" + url.QueryEscape(jobId))
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	var r Status
	err = json.NewDecoder(tee(res.Body, c.Debug)).Decode(&r)
	if err != nil {
		return nil, err
	}
	return &r, nil
}

func (c *client) JobResultFunc(jobId string, typ string, cb func(line string) error) error {
	res, err := c.get("/v3/job/result/" + url.QueryEscape(jobId) + "?format=" + url.QueryEscape(typ))
	if err != nil {
		return err
	}
	defer res.Body.Close()

	br := bufio.NewReader(tee(res.Body, c.Debug))
	for {
		lb, _, err := br.ReadLine()
		if err != nil {
			if err != io.EOF {
				return err
			}
			break
		}
		err = cb(string(lb))
		if err != nil {
			return nil
		}
	}
	return nil
}
