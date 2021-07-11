package redis

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/adg/go-project/src/util"

	redigo "github.com/garyburd/redigo/redis"
	cmap "github.com/orcaman/concurrent-map"
	"gopkg.in/eapache/go-resiliency.v1/breaker"
	gcfg "gopkg.in/gcfg.v1"
)

type (
	Database struct {
		Config     DatabaseConfig
		ClientList DatabaseClientList
	}

	DatabaseConfig struct {
		Initialized bool
		Setting     struct {
			IdleTimeout  int
			DialTimeout  int
			ReadTimeout  int
			WriteTimeout int
		}

		Connection map[string]*ConnectionDetail

		MaxIdle   map[string]*ConfigValueInt
		MaxActive map[string]*ConfigValueInt
	}

	ConnectionDetail struct {
		Master       string
		IdleTimeout  int
		DialTimeout  int
		ReadTimeout  int
		WriteTimeout int
	}

	ConfigValueInt struct {
		Value int
	}

	// Database Client
	DatabaseClientList struct {
		Name    map[string]DatabaseClientReplication
		Runtime map[string]RuntimeSettingRepl
	}

	DatabaseClientReplication struct {
		Master *redigo.Pool
	}

	RuntimeSettingRepl map[string]*RuntimeSetting

	RuntimeSetting struct {
		Active     bool
		LastReload time.Time
	}

	// Wrapper
	Connectionx struct {
		Conn redigo.Conn
		mu   *sync.Mutex
	}
)

var (
	DB Database = Database{
		ClientList: DatabaseClientList{
			Name:    make(map[string]DatabaseClientReplication, 0),
			Runtime: make(map[string]RuntimeSettingRepl, 0),
		},
	}

	GlobalLock = &sync.RWMutex{}
	RTSLock    = &sync.RWMutex{}

	// ERROR
	ErrNoConnection      = errors.New("No connection found")
	ErrDisableConnection = errors.New("Connection has beed disabled")
)

// PRIVATE METHOD
func configIntCoalesce(value ...int) int {
	var ret int
	for _, i := range value {
		if i < 0 {
			return 0
		} else if i > 0 {
			return i
		}

		ret = i
	}

	return ret
}

func (cfgValInt *ConfigValueInt) getValue() int {
	if cfgValInt == nil {
		return 0
	}

	return cfgValInt.Value
}

func getReplication(sRepl []string) string {
	var repl string

	uds := true
	if len(sRepl) > 0 {
		repl = sRepl[0]

		if repl == "master" {
			uds = false
		}
	}

	if uds {
		// default connection "master"
		repl = "master"
	}

	return repl
}

func getHostMap(clientName string) (DatabaseClientReplication, bool) {
	GlobalLock.RLock()
	defer GlobalLock.RUnlock()

	cli, exists := DB.ClientList.Name[clientName]

	return cli, exists
}

func getRuntimeSetting(clientName string, repName string) RuntimeSetting {
	RTSLock.RLock()
	defer RTSLock.RUnlock()

	_, exists := DB.ClientList.Runtime[clientName]
	if !exists {
		RTSLock.RUnlock()

		RTSLock.Lock()
		// check key for once again, to prevent multiple init from race condition
		if _, exists := DB.ClientList.Runtime[clientName]; !exists {
			DB.ClientList.Runtime[clientName] = make(map[string]*RuntimeSetting)
		}
		RTSLock.Unlock()

		RTSLock.RLock()
	}

	_, exists = DB.ClientList.Runtime[clientName][repName]
	if !exists {
		RTSLock.RUnlock()

		RTSLock.Lock()
		// check key for once again, to prevent multiple init from race condition
		if _, exists := DB.ClientList.Runtime[clientName][repName]; !exists {
			DB.ClientList.Runtime[clientName][repName] = &RuntimeSetting{Active: true}
		}
		RTSLock.Unlock()

		RTSLock.RLock()
	}

	return *DB.ClientList.Runtime[clientName][repName]
}

func setRuntimeSetting(clientName string, repName string, param string, value interface{}) (bool, error) {
	RTSLock.Lock()
	defer RTSLock.Unlock()

	if _, exists := DB.ClientList.Runtime[clientName]; !exists {
		DB.ClientList.Runtime[clientName] = make(map[string]*RuntimeSetting)
	}

	if _, exists := DB.ClientList.Runtime[clientName][repName]; !exists {
		DB.ClientList.Runtime[clientName][repName] = &RuntimeSetting{Active: true}
	}

	param = strings.ToLower(param)
	switch param {
	case "active":
		if b, ok := value.(bool); ok {
			DB.ClientList.Runtime[clientName][repName].Active = b
		}
	case "last_reload":
		if t, ok := value.(time.Time); ok {
			DB.ClientList.Runtime[clientName][repName].LastReload = t
		}
	}

	return false, errors.New("runtime set param not found")
}

func setHostStatus(clientName, repName string, status bool) {
	setRuntimeSetting(clientName, repName, "active", status)
}

func isActiveHost(clientName, repName string) bool {
	rs := getRuntimeSetting(clientName, repName)

	return rs.Active
}

func isRecentReloaded(clientName, repName string, timeDurationSec float64) bool {
	rs := getRuntimeSetting(clientName, repName)

	if iz := rs.LastReload.IsZero(); iz {
		return false
	}

	if time.Since(rs.LastReload).Seconds() <= timeDurationSec {
		return true
	}

	return false
}

// PUBLIC METHOD
func CreateConnection(clientName string, host string) *redigo.Pool {
	if clientName == "" || host == "" {
		return nil
	}

	if _, ok := DB.Config.Connection[clientName]; !ok {
		return nil
	}

	defaults := DB.Config.Setting
	connSetting := DB.Config.Connection[clientName]

	maxIdleClient := (DB.Config.MaxIdle[clientName]).getValue()
	maxIdleDefault := (DB.Config.MaxIdle["default"]).getValue()
	maxActiveClient := (DB.Config.MaxActive[clientName]).getValue()
	maxActiveDefault := (DB.Config.MaxActive["default"]).getValue()

	maxIdle := configIntCoalesce(maxIdleClient, maxIdleDefault)
	maxActive := configIntCoalesce(maxActiveClient, maxActiveDefault)
	idleTimeout := time.Duration(configIntCoalesce(connSetting.IdleTimeout, defaults.IdleTimeout)) * time.Second
	dialTimeout := time.Duration(configIntCoalesce(connSetting.DialTimeout, defaults.DialTimeout)) * time.Second
	readTimeout := time.Duration(configIntCoalesce(connSetting.ReadTimeout, defaults.ReadTimeout)) * time.Second
	writeTimeout := time.Duration(configIntCoalesce(connSetting.WriteTimeout, defaults.WriteTimeout)) * time.Second

	return &redigo.Pool{
		MaxIdle:     maxIdle,
		MaxActive:   maxActive,
		IdleTimeout: idleTimeout,
		Dial: func() (redigo.Conn, error) {
			dialOption := []redigo.DialOption{
				redigo.DialConnectTimeout(dialTimeout),
				redigo.DialReadTimeout(readTimeout),
				redigo.DialWriteTimeout(writeTimeout),
			}

			c, err := redigo.Dial("tcp", host, dialOption...)
			if err != nil {
				return nil, err
			}

			return c, err
		},
	}
}

// GetConnection return redis connection obtained from pool based on
// client name and replication type sent in parameter
func GetConnection(clientName string, sRepl ...string) (redigo.Conn, error) {
	pool, err := getPool(clientName, sRepl...)
	if err != nil {
		return nil, err
	}

	return pool.Get(), nil
}

func getPool(clientName string, sRepl ...string) (*redigo.Pool, error) {
	defer SCIncrement(clientName)
	var pool *redigo.Pool

	if !DB.Config.Initialized {
		return nil, ErrNoConnection
	}

	repl := getReplication(sRepl)

	cli, exists := getHostMap(clientName)
	if !exists {
		log.Println("redis getconnection err", ErrNoConnection, clientName)
		return nil, ErrNoConnection
	}

	if !isActiveHost(clientName, repl) {
		return nil, ErrDisableConnection
	}

	if repl == "master" {
		pool = cli.Master
	}

	if pool == nil {
		return nil, ErrNoConnection
	}

	return pool, nil
}

// NewDatabaseClient returns new database client for a given client name along with all of its replications (master only).
// When flag reload is set, the current connection for specified replication names will be closed before new connection is made.
// If no replication names are supplied, the master will be reloaded.
func NewDatabaseClient(clientName string, reload bool, repNames ...string) DatabaseClientReplication {
	GlobalLock.Lock()
	defer GlobalLock.Unlock()

	// register heartbeat
	// currently, redis only available on master
	defer RegisterHeartBeat(clientName, "master", 0)

	isRepReload := make(map[string]bool) // Marker for replication that needs to be reloaded.
	if reload {
		if cli, exists := DB.ClientList.Name[clientName]; exists {
			// Check if the replication need to be reloaded.
			// If so, close the connection pool and mark it.
			if len(repNames) == 0 || util.InArrayString("master", repNames) {
				cli.Master.Close()
				isRepReload["master"] = true
			} else {
				isRepReload["master"] = false
			}
		}
	}

	repl := DatabaseClientReplication{} // This will hold the new replications

	// Create/reuse connection
	// If flag reload is not set, create new connection for master.
	// If it doesn't, create new replication that need to be reloaded.
	if !reload || isRepReload["master"] {
		// Master need to be reloaded, create new connection for it.
		hostMaster := DB.Config.Connection[clientName].Master
		repl.Master = CreateConnection(clientName, hostMaster)
	} else {
		// Master doesn't need to be reloaded, reuse the connection.
		repl.Master = DB.ClientList.Name[clientName].Master
	}

	DB.ClientList.Name[clientName] = repl
	return repl
}

func NewDatabaseClientAll() error {
	if !DB.Config.Initialized {
		return errors.New("Config redis variable not initialized")
	}

	for clientName := range DB.Config.Connection {
		NewDatabaseClient(clientName, false)
	}

	return nil
}

func InitDatabaseConfig(filePath string) (DatabaseConfig, error) {
	if !DB.Config.Initialized {
		cfg := DatabaseConfig{Initialized: true}

		err := gcfg.ReadFileInto(&cfg, filePath)
		if err != nil {
			return cfg, err
		}

		DB.Config = cfg
	}

	return DB.Config, nil
}

// Database Heartbeat
var (
	HBTrackerEnabled    = false
	HBTrackerStopSignal = make(chan bool)
	HBRegisteredClient  = make([]string, 0)
	HeartBeatLock       = &sync.Mutex{}

	bpMutex           = &sync.Mutex{}
	IsRunningBeatPing = false

	deferMutex       = &sync.Mutex{}
	isDeferredClient = make(map[string]bool)

	ClientMap = cmap.New()
)

func BeatPing() {
	bpMutex.Lock()
	if IsRunningBeatPing {
		bpMutex.Unlock()
		return
	}
	IsRunningBeatPing = true
	bpMutex.Unlock()

	cp := make([]string, 0)
	HeartBeatLock.Lock()
	for _, client := range HBRegisteredClient {
		cp = append(cp, client)
	}
	HeartBeatLock.Unlock()

	timeDelay := 10

	for _, client := range cp {
		if spl := strings.Split(client, ":"); len(spl) == 2 {
			clientName, repl := spl[0], spl[1]
			cb_cmap, cb_cmap_exists := ClientMap.Get(client)
			if !cb_cmap_exists {
				cb_cmap = breaker.New(10, 5, time.Duration(timeDelay)*time.Second)
				ClientMap.Set(client, cb_cmap)
			}

			cb := cb_cmap.(*breaker.Breaker)
			res := cb.Run(func() error {
				cli, err := GetConnection(clientName, repl)
				if err != nil {
					return err
				}
				defer cli.Close()
				values, err := redigo.String(cli.Do("PING"))
				if err != nil {
					// let's override return for production testing, since err pool mostly is not expected
					// the only disadvantage is spike traffic could cause instant breaker (sensitive breaker)
					return err

					// if err == redigo.ErrPoolExhausted {
					// 	// some unclosed connection pool detected or full connection, let's wait
					// 	log.Println(clientName, repl, err)
					// 	return nil
					// } else if err.Error() == "redigo: get on closed pool" {
					// 	return err
					// }
				}

				if values == "PONG" {
					return nil
				}

				// no clue what happen, still go to circuit breaker
				return errors.New("beat ping undefined error")
			})

			if res == breaker.ErrBreakerOpen {
				if isActiveHost(clientName, repl) && !isRecentReloaded(clientName, repl, float64(timeDelay)) {
					log.Println("[redis] circuit breaker opens", clientName, repl)
					RemoveHeartBeat(client)
					setHostStatus(clientName, repl, false)
					go deferredReloadClient(client, true, timeDelay)
				}
			} else if res == ErrDisableConnection {
				RemoveHeartBeat(client)
			} else if res == ErrNoConnection {
				// connection not initialized yet
				RemoveHeartBeat(client)
				go deferredReloadClient(client, false, timeDelay)
			}
		}
	}

	bpMutex.Lock()
	IsRunningBeatPing = false
	bpMutex.Unlock()
}

func deferredReloadClient(client string, reload bool, timeDelay int) {
	if spl := strings.Split(client, ":"); len(spl) == 2 {
		// check lock status if already registered to be reloaded
		deferMutex.Lock()
		isAlreadyDeferred := isDeferredClient[client]
		if isAlreadyDeferred {
			deferMutex.Unlock()
			return
		}
		isDeferredClient[client] = true
		deferMutex.Unlock()

		// running defer reload process
		clientName, repl := spl[0], spl[1]
		time.Sleep(time.Duration(10) * time.Second)
		log.Println("[redis] reload", clientName, repl)
		NewDatabaseClient(clientName, true, repl)
		setRuntimeSetting(clientName, repl, "last_reload", time.Now())
		setHostStatus(clientName, repl, true)

		ClientMap.Set(client, breaker.New(10, 5, time.Duration(timeDelay)*time.Second))

		// set finish lock
		deferMutex.Lock()
		isDeferredClient[client] = false
		deferMutex.Unlock()
	}
}

func isRegisteredHB(hbName string) bool {
	for _, registeredName := range HBRegisteredClient {
		if registeredName == hbName {
			return true
		}
	}

	return false
}

func RegisterHeartBeat(clientName string, repl string, num int) {
	defer HeartBeatLock.Unlock()
	HeartBeatLock.Lock()
	if num > 0 {
		for i := 0; i < num; i++ {
			if hbName := fmt.Sprintf("%s:%s_%d", clientName, repl, i+1); !isRegisteredHB(hbName) {
				HBRegisteredClient = append(HBRegisteredClient, hbName)
			}
		}
	} else {
		if hbName := fmt.Sprintf("%s:%s", clientName, repl); !isRegisteredHB(hbName) {
			HBRegisteredClient = append(HBRegisteredClient, hbName)
		}
	}
}

func RemoveHeartBeat(hbName string) {
	defer HeartBeatLock.Unlock()
	HeartBeatLock.Lock()
	for i, registeredName := range HBRegisteredClient {
		if registeredName == hbName {
			HBRegisteredClient = append(HBRegisteredClient[:i], HBRegisteredClient[i+1:]...)
		}
	}
}

func StartHeartBeat() {
	defer HeartBeatLock.Unlock()
	HeartBeatLock.Lock()
	if !HBTrackerEnabled {
		HBTrackerEnabled = true

		ticker := time.NewTicker(time.Duration(1) * time.Second)
		go func() {
			for {
				select {
				case <-ticker.C:
					BeatPing()
				case <-HBTrackerStopSignal:
					HBTrackerEnabled = false
					return
				}
			}
		}()
	}
}
