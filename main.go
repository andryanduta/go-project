package main

import (
	"errors"
	"fmt"
	"log"
	"log/syslog"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/adg/go-project/src/core"
	"github.com/adg/go-project/src/util"
	"github.com/adg/go-project/v1/pkg/handlerutil"
)

// if using very long module, increase the %s length
func loader(moduleName string, elapsedTime float64) {
	log.Println(fmt.Sprintf("[loader] > %-25s %.09f", moduleName, elapsedTime))
}

func main() {

	// SetFlags sets the default output for standard logs.
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	timer := util.NewTimer()
	timer.StartTimer()

	// Get environment variable
	environ := os.Getenv("BINARYENV")
	if environ == "" {
		environ = "development"
	}

	// Init project directory path
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		log.Fatalln(err)
	}

	// Set directory addresses
	mainIniFile := "go-main.ini"
	baseDir := fmt.Sprintf("/%s/", strings.Trim(dir, "/"))
	fdir := baseDir + "/files/etc/go-project/"
	sdir := fdir + fmt.Sprintf("config/%s/dashboard/", environ)

	// Initialize config
	mConf, err := core.NewMainConfig(sdir+mainIniFile, baseDir)
	if err != nil {
		log.Fatalln("Error initialize main config", err)
	}

	if mConf.Server.Syslog == true {
		logWriter, err := syslog.New(syslog.LOG_NOTICE, "go-project")
		if err == nil {
			log.SetOutput(logWriter)
		}
	}
	confTime := timer.GetElapsedTime()
	log.Println(fmt.Sprintf("%-11s%-25s %s", "", "Module Name", "Load Time (sec)"))
	loader("Config", confTime)

	// Database init
	_, err = core.InitDBClient(core.DBGlobalSetting{
		Apps:            "dashboard",
		Environment:     mConf.Server.Env,
		EnableProfile:   mConf.Server.DBProfile,
		EnableHeartBeat: mConf.Server.DBHeartBeat,
		EnableStatistic: mConf.Server.EnableDBStatistic,
	}, sdir+"database/", "postgresql", "redis")
	loader("Database", timer.GetElapsedTime())
	if err != nil {
		dbErr := errors.New("Error initializing databases, please check server log for details")
		log.Fatalln(dbErr)
	}

	// start all handlers
	handlerutil.Start()

	// Start server
	port := fmt.Sprintf(":%d", mConf.Server.Port)

	loader("HandlerServe", timer.GetElapsedTime())

	log.Fatal(http.ListenAndServe(port, nil))
}
