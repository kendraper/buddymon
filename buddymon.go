package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/influxdata/influxdb/client/v2"
)

const buddyPath = "proc_buddyinfo.txt"
const assertFieldCount = 15 // requisite fields in each buddyinfo line

var config struct {
	Filename     string
	InfluxSeries string
	InfluxTags   map[string]interface{}
	InfluxURL    string
}

func init() {
	viper.SetConfigName("buddymon")

	pflag.StringP("config", "c", "", "Config file path (default searches /etc/buddymon, $HOME/buddymon, $PWD)")
	pflag.StringP("series", "s", "buddyinfo", "InfluxDB series name to update")
	pflag.StringP("url", "u", "http://localhost:8086", "InfluxDB URL")
	tags := pflag.StringSliceP("tags", "t", nil, "InfluxDB tags to add, e.g. host=mycomputer (multiple -t or commas ok)")
	pflag.Parse()

	viper.BindPFlags(pflag.CommandLine)

	configFile := viper.GetString("config")
	if configFile == "" {
		// Option -c not specified, search default paths for config file.
		viper.AddConfigPath(".")
		viper.AddConfigPath("$HOME/.buddymon")
		viper.AddConfigPath("/etc/buddymon/")
	} else {
		viper.SetConfigFile(configFile)
	}

	err := viper.ReadInConfig()
	if err == nil {
		viper.WatchConfig()
		viper.OnConfigChange(func(e fsnotify.Event) {
			log.Println("Configuration reloaded:", e.Name)
		})
	}

	allSettings := viper.AllSettings()
	log.Printf("Settings: %q\n", allSettings)

	config.InfluxTags = viper.GetStringMap("tags")
	if len(config.InfluxTags) == 0 {
		// Build tags from command line -t
		if len(*tags) > 0 {
			for _, tagset := range *tags {
				tag := strings.SplitN(tagset, "=", 2)
				if len(tag) != 2 {
					fmt.Fprintf(os.Stderr, "ERROR: Invalid tag '%s', use syntax tag=value\n", tagset)
					pflag.Usage()
					os.Exit(8)
				}
				config.InfluxTags[tag[0]] = tag[1]
			}
		}
	}
	log.Printf("Tags: %q\n", config.InfluxTags)
}

func main() {
	lines, err := slurpLines(buddyPath)
	if err != nil {
		log.Fatal(err)
	}

	for _, line := range lines {
		fields := getInfluxEntry(line)
		log.Printf("fields %v\n", fields)
		tags := map[string]string{"cpu": "cpu-total"}
		updateInflux(tags, fields, "http://bink:8086")
	}
}

func updateInflux(tags map[string]string, fields map[string]interface{}, url string) {
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     url,
		Username: "",
		Password: "",
	})
	if err != nil {
		log.Fatal(err)
	}

	// Create a new point batch
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  "buddyinfo",
		Precision: "s",
	})
	if err != nil {
		log.Fatal(err)
	}

	pt, err := client.NewPoint("cpu_usage", tags, fields, time.Now())
	if err != nil {
		log.Fatal(err)
	}
	bp.AddPoint(pt)

	// Write the batch
	if err := c.Write(bp); err != nil {
		log.Fatal(err)
	}
}

/*
Buddyinfo sample. All rows may not be present.
See: https://www.kernel.org/doc/Documentation/filesystems/proc.txt

> cat /proc/buddyinfo
Node 0, zone      DMA      1      1      1      0      2      1      1      0      1      1      3
Node 0, zone    DMA32      3      6      5      3      3      4      2      4      3      1    270
Node 0, zone   Normal  23821   5715     90     16      8      4      9      2      0      0      0
Node 1, zone   Normal   3888  10304    405    139     50     59     38     19      4      2      9
*/

// Given a buddyinfo line, returns a field map for InfluxDB.
func getInfluxEntry(line string) map[string]interface{} {
	fields := strings.Fields(line)
	n := len(fields)
	if n != assertFieldCount {
		panic(fmt.Sprintf(
			"Found %d fields in %s (expected %d), offending line follows:\n%s\n",
			n, buddyPath, assertFieldCount, line))
	}
	node := fields[1][0] // extract e.g. 0 from "0,"
	zone := fields[3]    // zone type, e.g. Normal
	pages := fields[4:]  // all subsequent fragment counts

	entry := map[string]interface{}{
		"node": string(node),
		"zone": string(zone),
	}
	pageOrder := 1
	for _, p := range pages {
		name := fmt.Sprintf("%dp", pageOrder)
		entry[name] = string(p)
		pageOrder *= 2
	}

	return entry
}

func slurpLines(path string) ([]string, error) {
	var lines []string

	data, err := ioutil.ReadFile(path)
	if err != nil {
		return lines, err
	}

	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	return lines, nil
}
