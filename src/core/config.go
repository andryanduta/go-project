package core

import (
	"errors"
	"gopkg.in/gcfg.v1"
	"strings"
)

type (
	ServerStruct struct {
		Env               string
		Port              int
		ProfilerPort      int
		RootDir           string
		Syslog            bool
		UseToken          bool
		DBProfile         bool
		DBHeartBeat       bool
		EnableDBStatistic bool
		ApiProfile        bool
	}
	// Define struct for main config
	MainConfig struct {
		Server ServerStruct
	}
)

var (
	MainCfg *MainConfig
)

func NewMainConfig(filePath string, baseDir string) (*MainConfig, error) {

	var (
		customErr error
	)

	if MainCfg == nil {

		var mc MainConfig

		err := gcfg.ReadFileInto(&mc, filePath)
		if err != nil {

			errDesc := err.Error()
			customErr = errors.New("Could not load config file: " + filePath + errDesc)

			if !strings.Contains(errDesc, "can't store data at section") {
				return nil, customErr
			}
		}

		// set default value if config is missing
		SetDefaultValue(&mc)

		// parse config format
		parseConfig(&mc)

		mc.Server.RootDir = baseDir

		MainCfg = &mc
	}

	return MainCfg, customErr
}

func parseConfig(mc *MainConfig) {
	if mc == nil {
		return
	}
}

// SetDefaultValue set default value if config is missing
func SetDefaultValue(mc *MainConfig) {

}

// GetConfig retrieves main config data
var GetConfig = func() (*MainConfig, error) {
	if MainCfg == nil {
		return nil, errors.New("Config is empty")
	}
	return MainCfg, nil
}
