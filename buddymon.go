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

// InfluxSettings stores the required configuration to write data points to InfluxDB.
type InfluxSettings struct {
	URL         string
	Database    string
	User        string
	Password    string
	Measurement string // Measurement name in "SELECT ___ FROM measurement_name"
	GlobalTags  map[string]string
}

// Global config.
var influxConfig InfluxSettings

// BuddyEntry binds a set of page entries to node number and zone.
type BuddyEntry struct {
	Pages map[string]interface{} // Matches fields arg of InfluxDB data point.
	Node  string
	Zone  string
}

func init() {
	viper.SetConfigName("buddymon")

	pflag.StringP("config", "c", "", "Config file path (default searches /etc/buddymon, $HOME/buddymon, $PWD)")
	pflag.StringP("url", "U", "http://localhost:8086", "InfluxDB server URL")
	pflag.StringP("database", "d", "buddyinfo", "InfluxDB database name to use")
	pflag.StringP("user", "u", "", "InfluxDB username for writing")
	pflag.StringP("password", "p", "", "InfluxDB password for user authentication")
	pflag.StringP("measurement", "m", "buddyinfo", "InfluxDB measurement name to write")
	tags := pflag.StringSliceP("tags", "t", []string{}, "InfluxDB tags to add, e.g. host=mycomputer (multiple -t or commas ok)")
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

	// Set config options.
	influxConfig.URL = viper.GetString("url")
	influxConfig.Database = viper.GetString("database")
	influxConfig.User = viper.GetString("user")
	influxConfig.Password = viper.GetString("password")
	influxConfig.Measurement = viper.GetString("measurement")

	influxConfig.GlobalTags = viper.GetStringMapString("tags")
	if len(influxConfig.GlobalTags) == 0 {
		// Build tags from command line -t if we received them (key=val strings).
		if len(*tags) > 0 {
			for _, tagset := range *tags {
				tag := strings.SplitN(tagset, "=", 2)
				if len(tag) != 2 {
					fmt.Fprintf(os.Stderr, "ERROR: Invalid tag '%s', use syntax tag=value\n", tagset)
					pflag.Usage()
					os.Exit(8)
				}
				influxConfig.GlobalTags[tag[0]] = tag[1]
			}
		}
	}
	log.Printf("[1]GlobalTags: %q\n", influxConfig.GlobalTags)

	// TODO: Check for required options

	allSettings := viper.AllSettings()
	log.Printf("ViperSettings: %q\n", allSettings)
}

func main() {
	lines, err := slurpLines(buddyPath)
	if err != nil {
		log.Fatal(err)
	}

	var batch []BuddyEntry
	for _, line := range lines {
		entry := makeBuddyEntry(line)
		log.Printf("entry %v\n", entry)
		batch = append(batch, entry)
	}
	log.Printf("batch %v\n", batch)
	updateInflux(influxConfig, batch)
}

// func updateInflux(tags map[string]string, fields map[string]interface{}, url string) {
func updateInflux(influx InfluxSettings, batch []BuddyEntry) {
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     influx.URL,
		Username: influx.User,
		Password: influx.Password,
	})
	if err != nil {
		log.Fatal(err)
	}

	// Create a new point batch.
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  influx.Database,
		Precision: "ns",
	})
	if err != nil {
		log.Fatal(err)
	}

	// Time will be incremented by a nanosecond per each data point, to
	// prevent multiple points from clobbering each other.
	// Since time.Now() does not have nanosecond precision on all OSes, running
	// it in a loop can easily net identical times.
	t := time.Now()

	// Add a point for each field set in the batch.
	for _, entry := range batch {
		// Add provided tags where applicable.
		tags := influx.GlobalTags
		tags["node"] = entry.Node
		tags["zone"] = entry.Zone

		pt, err := client.NewPoint(influx.Measurement, tags, entry.Pages, t)
		if err != nil {
			log.Fatal(err)
		}
		bp.AddPoint(pt)

		t = t.Add(time.Nanosecond)
	}

	// Write the batch.
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

// Given a buddyinfo line, returns both a key and field map for InfluxDB.
// Node number and zone should be handled as tags and not fields, since those
// may be frequently queried (fields are not indexed).
func makeBuddyEntry(line string) (entry BuddyEntry) {
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

	entry = BuddyEntry{}
	entry.Node = string(node)
	entry.Zone = string(zone)
	entry.Pages = make(map[string]interface{})

	pageOrder := 1
	for _, p := range pages {
		name := fmt.Sprintf("%dp", pageOrder)
		entry.Pages[name] = string(p)
		// influxFields[name] = string(p)
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
