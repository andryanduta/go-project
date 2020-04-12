package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"log/syslog"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/adg/go-project/src/core"
	"github.com/adg/go-project/src/util"
	"github.com/adg/go-project/v2/pkg/handlerutil"
)

// if using very long module, increase the %s length
func loader(moduleName string, elapsedTime float64) {
	log.Println(fmt.Sprintf("[loader] > %-25s %.09f", moduleName, elapsedTime))
}

func main() {
	configtest := flag.Bool("test", false, "config test")

	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lshortfile)

	timer := util.NewTimer()
	timer.StartTimer()

	environ := os.Getenv("TKPENV")
	if environ == "" {
		environ = "development"
	}

	// Initialize directory path
	baseDir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		log.Fatalln(err)
	}

	// If using go run
	if strings.Contains(baseDir, "tmp") {
		if baseDir, err = os.Getwd(); err != nil {
			log.Fatalln(err)
		}
	}

	// Set directory addresses
	mainIniFile := "go-main.ini"
	fdir := baseDir + "/files/etc/go-project/"
	sdir := fdir + fmt.Sprintf("config/%s/dashboard/", environ)

	// Initialize config
	mConf, err := core.NewMainConfig(sdir+mainIniFile, baseDir)
	if err != nil {
		log.Fatalln("Error initialize main config", err)
	}

	if !*configtest {
		if mConf.Server.Syslog == true {
			logWriter, err := syslog.New(syslog.LOG_NOTICE, "go-project")
			if err == nil {
				log.SetOutput(logWriter)
			}
		}
		confTime := timer.GetElapsedTime()
		log.Println(fmt.Sprintf("%-11s%-25s %s", "", "Module Name", "Load Time (sec)"))
		loader("Config", confTime)
	}

	// Database init
	_, err = core.InitDBClient(core.DBGlobalSetting{
		Apps:            "dashboard",
		Environment:     mConf.Server.Env,
		EnableProfile:   mConf.Server.DBProfile,
		EnableHeartBeat: mConf.Server.DBHeartBeat,
		EnableStatistic: mConf.Server.EnableDBStatistic,
	}, sdir+"database/", "postgresql", "redis", "nsq")
	loader("Database", timer.GetElapsedTime())
	if err != nil {
		dbErr := errors.New("Error initializing databases, please check server log for details")
		log.Fatalln(dbErr)
	}

	//Exit if test-flag is given
	if *configtest {
		os.Exit(0)
	}

	// start all handlers
	handlerutil.Start()

	// Start server
	port := fmt.Sprintf(":%d", mConf.Server.Port)

	loader("HandlerServe", timer.GetElapsedTime())

	log.Fatal(http.ListenAndServe(port, nil))
}
