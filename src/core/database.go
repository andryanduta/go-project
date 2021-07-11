package core

import (
	"errors"
	"log"

	"github.com/adg/go-project/src/util/database/nsq"
	"github.com/adg/go-project/src/util/database/postgresql"
	"github.com/adg/go-project/src/util/database/redis"
)

type (
	// DatabaseConfig defines struct for database
	DatabaseConfig struct {
		PostgreSQLDB *postgresql.DatabaseConfig
		RedisDB      *redis.DatabaseConfig
		Nsq          *nsq.NsqConfig
	}
	// DBGlobalSetting defines struct for the setting
	DBGlobalSetting struct {
		Apps            string
		Environment     string
		EnableProfile   bool
		EnableHeartBeat bool
		EnableStatistic bool
	}
)

// InitDBClient initialize database client
func InitDBClient(dbs DBGlobalSetting, dbConfDir string, listAppName ...string) (*DatabaseConfig, error) {
	var err error
	dbCfg := &DatabaseConfig{}

	errList := []string{}
	if len(listAppName) == 0 {
		listAppName = []string{"postgresql", "redis", "nsq"}
	}

	// Load config
	for _, appName := range listAppName {
		switch appName {
		case "postgresql":
			{
				conf, err := postgresql.InitDatabaseConfig(dbConfDir + "postgresql.ini")
				if err != nil {
					errList = append(errList, "postgresql")
					log.Println("[InitDBClient] postgresql.InitDatabaseConfig process failed ", err)
				} else {
					dbCfg.PostgreSQLDB = &conf
					err = postgresql.NewDatabaseClientAll()
					if err != nil {
						errList = append(errList, "postgresql")
					}
					if dbs.EnableHeartBeat {
						postgresql.StartHeartBeat()
					}

				}
			}
		case "redis":
			{
				conf, err := redis.InitDatabaseConfig(dbConfDir + "redis.ini")
				if err != nil {
					errList = append(errList, "redis")
					log.Println("[InitDBClient] redis.InitDatabaseConfig process failed ", err)
				} else {
					dbCfg.RedisDB = &conf
					err = redis.NewDatabaseClientAll()
					if err != nil {
						log.Println("[InitDBClient] redis.InitDatabaseConfig process failed ", err)
						errList = append(errList, "redis")
					}

					if dbs.EnableHeartBeat {
						redis.StartHeartBeat()
					}
				}
			}
		case "nsq":
			{
				conf, err := nsq.NewNsqConfig(dbConfDir + "nsq.ini")
				if err != nil {
					log.Println("[InitDBClient] NewNsqConfig process failed ", err)
					errList = append(errList, "nsq")
				} else {
					dbCfg.Nsq = conf
					err = nsq.NewNsqProducerAll()
					if err != nil {
						errList = append(errList, "nsq")
						log.Println("[InitDBClient] NewNsqConfig process failed ", err)
					}
				}
			}
		}
	}
	if len(errList) > 0 {
		var errStr string
		for _, tmpErr := range errList {
			errStr = errStr + tmpErr + "\n"
		}
		err = errors.New(errStr)
	}
	return dbCfg, err
}
