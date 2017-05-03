package ora

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/internal/errchan"
	"github.com/influxdata/telegraf/plugins/inputs"
	ora "gopkg.in/rana/ora.v4"
)

type Ora struct {
	Url   string   `toml:"url"`
	Files []string `toml:"files"`
	Dbid  string   `toml:"dbid"`

	sync.Mutex
	sqlmap map[string][]string
	u      *url
}

type url struct {
	all      string
	user     string
	passwd   string
	host     string
	port     string
	service  string
	instance string
}

var sampleConfig = `
  ## specify OCI connection URL
  ## URL are separated into multiple tags(orahost,oraport,oraservice,orainstance)
  ## see - http://docs.oracle.com/database/121/NETAG/naming.htm#NETAG255
  ##   [user][/password][@]host:port/oracle_service_name[:pooled]
  ##   [user][/password][@]host:port/oracle_service_name[:pooled] as sysdba 
  ## for example:
  url = "perfstat/perfstat@localhost:1521/orcl"
  ## specify measure the associated SQLs files
  ## SQL file Content format:  SQL-name::SQL-Statement;;
  files = ["default.sql"]
  ## additional labels, override tag in SQL.
  dbid = "orcl"
`

func (o *Ora) Description() string {
	return "Read metrics from one Oracle Databases."
}

func (o *Ora) SampleConfig() string {
	return sampleConfig
}

func (o *Ora) Gather(acc telegraf.Accumulator) error {
	o.Lock()
	defer o.Unlock()

	o.sqlmap = make(map[string][]string)
	err := o.readfiles()
	if err != nil {
		return err
	}

	conn, err := sql.Open("ora", o.Url)
	if err != nil {
		return err
	}
	defer conn.Close()

	//生成URL标签
	o.tagUrl()

	var ln int
	for _, v := range o.sqlmap {
		ln = ln + len(v)
	}

	var wg sync.WaitGroup
	errChan := errchan.New(ln)
	for tag, ss := range o.sqlmap {
		for _, s := range ss {
			wg.Add(1)

			go func(conn *sql.DB, tag string, s string) {
				defer wg.Done()

				errChan.C <- o.gatherInfo(acc, conn, tag, s)
			}(conn, tag, s)
		}
	}
	wg.Wait()

	return errChan.Error()
}

func (o *Ora) gatherInfo(acc telegraf.Accumulator, conn *sql.DB, tag string, sta string) error {
	var rowData = make(map[string]*interface{})
	var rowVars []interface{}

	rowset, err := conn.Query(sta)
	if err != nil {
		return err
	}

	colNames, err := rowset.Columns()
	for _, col := range colNames {
		rowData[col] = new(interface{})
		rowVars = append(rowVars, rowData[col])
	}

	for rowset.Next() {
		if err := rowset.Scan(rowVars...); err != nil {
			return err
		}
		tags, fields, err := o.parseRow(rowData)
		if err != nil {
			return err
		}

		tags["func"] = tag
		acc.AddFields("ora", fields, tags)
	}
	return nil
}

func (o *Ora) parseRow(rowData map[string]*interface{}) (map[string]string, map[string]interface{}, error) {
	var tags = make(map[string]string)
	var fields = make(map[string]interface{})
	var err error

	for k, v := range rowData {
		if v == nil {
			continue
		}

		k = strings.ToLower(k)
		switch val := (*v).(type) {
		case string:
			if val == "" {
				val = "NULL"
			}
			tags[k] = val
		case []byte:
			tags[k] = string(val)
		case int64, int32, int, float32, float64:
			fields[k] = val
		case ora.OCINum:
			n, _ := strconv.ParseFloat(val.String(), 64)
			fields[k] = n
		case bool:
			tags[k] = fmt.Sprintf("%b", val)
		default:
			log.Printf("I! parseRow column=%s type %T not support", k, val)
		}

		//添加DBID标签
		if len(strings.TrimSpace(o.Dbid)) > 0 {
			tags["dbid"] = o.Dbid
		}

		//添加URL生成标签
		if len(o.u.host) > 0 {
			tags["orahost"] = o.u.host
		}

		if len(o.u.port) > 0 {
			tags["oraport"] = o.u.port
		}

		if len(o.u.service) > 0 {
			tags["oraservice"] = o.u.service
		}

		if len(o.u.instance) > 0 {
			tags["orainstance"] = o.u.instance
		}
	}

	return tags, fields, err
}

func (o *Ora) tagUrl() {
	user := `([a-zA-z]+)`
	pass := `([a-zA-z]+)?`
	ip := `((?:(?:[0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}(?:25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9][0-9]|[0-9]))`
	port := `([0-9]+)?`
	service := `([a-zA-Z0-9]+)?`
	instance := `([a-zA-Z0-9]+)?`

	re := user + "/?" + pass + "@" + ip + ":" + port + "/?" + service + "/?" + instance
	r := regexp.MustCompile(re)

	matches := r.FindStringSubmatch(o.Url)

	o.u = &url{
		all:      matches[0],
		user:     matches[1],
		passwd:   matches[2],
		host:     matches[3],
		port:     matches[4],
		service:  matches[5],
		instance: matches[6],
	}
}

func (o *Ora) readfiles() error {
	var errChan = errchan.New(len(o.Files))

	for _, file := range o.Files {
		bs, err := ioutil.ReadFile(file)
		if err != nil {
			errChan.C <- err
			continue
		}

		rs := strings.Split(string(bs), ";;")

		for _, r := range rs {
			if len(strings.TrimSpace(r)) == 0 {
				continue
			}

			fs := strings.Split(r, "::")
			if fs == nil || len(fs) != 2 {
				log.Printf("I! SQL `%s` format error", r)
				continue
			}

			k := strings.TrimSpace(fs[0])
			v := strings.TrimSpace(fs[1])
			if len(k) == 0 || len(v) == 0 {
				continue
			}

			o.sqlmap[k] = append(o.sqlmap[k], v)
		}
	}

	//注释条目
	for k, _ := range o.sqlmap {
		if strings.HasPrefix(k, "#") {
			delete(o.sqlmap, k)
		}
	}

	return errChan.Error()
}

func init() {
	inputs.Add("ora",
		func() telegraf.Input {
			return &Ora{}
		})
}
