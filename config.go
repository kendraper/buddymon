package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// InfluxSettings stores the required configuration to write data points to InfluxDB.
type InfluxSettings struct {
	URL         string
	Database    string
	User        string
	Password    string
	Measurement string // Measurement name in "SELECT ___ FROM measurement_name"
	Hostname    string // Local hostname
	UseHostname bool
	GlobalTags  map[string]string
}

func getConfig() InfluxSettings {
	viper.SetConfigName("buddymon")

	defaultHost, err := os.Hostname()
	if err != nil {
		defaultHost = ""
	}
	defaultHost = strings.ToLower(defaultHost)

	pflag.StringP("config", "c", "", "Config file path (default searches /etc/buddymon, $HOME/buddymon, $PWD)")
	pflag.StringP("url", "U", "http://localhost:8086", "InfluxDB server URL")
	pflag.StringP("database", "d", "buddyinfo", "InfluxDB database name to use")
	pflag.StringP("user", "u", "", "InfluxDB username for writing")
	pflag.StringP("password", "p", "", "InfluxDB password for user authentication")
	pflag.StringP("hostname", "h", defaultHost, "Alternate hostname to use in 'host' tag (-H to bypass)")
	pflag.BoolP("no-hostname", "H", false, "Do not log a 'host' tag to InfluxDB")
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

	err = viper.ReadInConfig()
	if err == nil {
		viper.WatchConfig()
		viper.OnConfigChange(func(e fsnotify.Event) {
			log.Println("Configuration reloaded:", e.Name)
		})
	}

	// Set config options.
	var influxConfig InfluxSettings
	influxConfig.URL = viper.GetString("url")
	influxConfig.Database = viper.GetString("database")
	influxConfig.User = viper.GetString("user")
	influxConfig.Password = viper.GetString("password")
	influxConfig.Measurement = viper.GetString("measurement")
	influxConfig.Hostname = viper.GetString("hostname")
	influxConfig.UseHostname = !viper.GetBool("no-hostname")

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

	if influxConfig.UseHostname == true {
		influxConfig.GlobalTags["host"] = influxConfig.Hostname
	}
	return influxConfig
}
