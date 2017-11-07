package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"strings"
	"time"

	"github.com/influxdata/influxdb/client/v2"
)

const buddyPath = "proc_buddyinfo.txt"
const assertFieldCount = 15 // requisite fields in each buddyinfo line

var influxConfig InfluxSettings

func init() {
	influxConfig = getConfig()
}

// BuddyEntry binds a set of page entries to node number and zone.
type BuddyEntry struct {
	Pages map[string]interface{} // Matches fields arg of InfluxDB data point.
	Node  string
	Zone  string
}

func main() {
	for {
		if err := processBuddyInfo(); err != nil {
			fmt.Println("ERROR:", err)
		}
		time.Sleep(influxConfig.Interval)
	}
}

func processBuddyInfo() error {
	lines, err := slurpLines(buddyPath)
	if err != nil {
		log.Fatal(err)
	}

	var batch []BuddyEntry
	for _, line := range lines {
		entry, err := makeBuddyEntry(line)
		if err != nil {
			return err
		}
		batch = append(batch, entry)
	}
	return updateInflux(influxConfig, batch)
}

func updateInflux(influx InfluxSettings, batch []BuddyEntry) error {
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     influx.URL,
		Username: influx.User,
		Password: influx.Password,
	})
	if err != nil {
		return err
	}

	// Create a new point batch.
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  influx.Database,
		Precision: "ns",
	})
	if err != nil {
		return err
	}

	// Time will be incremented by a nanosecond per each data point, to
	// prevent multiple points from clobbering each other.
	// Since time.Now() does not have nanosecond precision on all OSes, running
	// it in a loop can easily net identical times.
	//
	// NOTE: Now storing node/zone as tags instead of fields, which should
	// prevent the overwrite, but it doesn't hurt to leave the increment in just
	// in case.
	//
	// See https://docs.influxdata.com/influxdb/v1.3/troubleshooting/frequently-asked-questions/#how-does-influxdb-handle-duplicate-points
	t := time.Now()

	// Add a point for each field set in the batch.
	for _, entry := range batch {
		tags := influx.GlobalTags
		tags["node"] = entry.Node
		tags["zone"] = entry.Zone

		pt, err := client.NewPoint(influx.Measurement, tags, entry.Pages, t)
		if err != nil {
			return err
		}
		bp.AddPoint(pt)

		t = t.Add(time.Nanosecond)
	}

	return c.Write(bp)
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

// Given a buddyinfo line, returns a field map for InfluxDB with node and zone.
// Node number and zone should be handled as tags and not fields, since those
// may be frequently queried (fields are not indexed).
func makeBuddyEntry(line string) (entry BuddyEntry, err error) {
	fields := strings.Fields(line)
	n := len(fields)
	if n != assertFieldCount {
		return entry, fmt.Errorf(
			"found %d fields in %s (expected %d) in %v",
			n, buddyPath, assertFieldCount, line)
	}
	node := fields[1][0] // extract e.g. 0 from "0,"
	zone := fields[3]    // zone type, e.g. Normal
	pages := fields[4:]  // all subsequent fragment counts

	entry = BuddyEntry{}
	entry.Node = string(node)
	entry.Zone = string(zone)
	entry.Pages = make(map[string]interface{})

	// See proc(5) for info on order (search buddyinfo).
	pageOrder := 1
	for _, p := range pages {
		name := fmt.Sprintf("%dp", pageOrder)
		entry.Pages[name] = string(p)
		pageOrder *= 2
	}

	return entry, nil
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
