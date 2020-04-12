package postgresql

import (
	"context"
	"errors"
	"fmt"
	"github.com/jmoiron/sqlx"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/adg/go-project/src/util"

	_ "github.com/lib/pq"
	"github.com/orcaman/concurrent-map"
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
			TimeoutSec             int
			ContextTimeoutMilliSec int
		}

		Connection map[string]*ConnectionDetail

		MaxLifeTime map[string]*ConfigValueInt
		MaxIdle     map[string]*ConfigValueInt
		MaxOpen     map[string]*ConfigValueInt
	}

	ConnectionDetail struct {
		Master     string
		Slave      []string
		TimeoutSec int
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
		Master *sqlx.DB
		Slave  []*sqlx.DB
	}

	RuntimeSettingRepl map[string]*RuntimeSetting

	RuntimeSetting struct {
		Active     bool
		LastReload time.Time
	}

	// Wrapper
	Connectionx struct {
		Conn *sqlx.DB
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

	// Database Heartbeat
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
		} else {
			if repl == "slave" {
				uds = false
			} else {
				s := strings.Split(repl, "_")
				if len(s) > 1 && s[0] == "slave" && s[1] != "" {
					idx, err := strconv.Atoi(s[1])
					if err == nil && idx > 0 {
						// reconstruct string to prevent unwanted connection like slave_2_x
						repl = fmt.Sprintf("%s_%s", s[0], s[1])
						uds = false
					}
				}
			}
		}
	}

	if uds {
		// default connection "master" or "slave"
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
func CreateConnection(clientName string, host string) (*sqlx.DB, error) {
	if clientName == "" || host == "" {
		return nil, errors.New("Failed to create postgresql connection, no client or host given")
	}

	if host == "master_mock" || host == "slave_mock" {
		return CreateConnectionMock(clientName, host, "postgres")
	}

	if _, ok := DB.Config.Connection[clientName]; !ok {
		return nil, errors.New("Failed to create postgresql connection, config not found")
	}

	defaults := DB.Config.Setting
	connSetting := DB.Config.Connection[clientName]

	timeoutSec := configIntCoalesce(connSetting.TimeoutSec, defaults.TimeoutSec)

	maxLifeTimeClient := (DB.Config.MaxLifeTime[clientName]).getValue()
	maxLifeTimeDefault := (DB.Config.MaxLifeTime["default"]).getValue()
	maxIdleClient := (DB.Config.MaxIdle[clientName]).getValue()
	maxIdleDefault := (DB.Config.MaxIdle["default"]).getValue()
	maxOpenClient := (DB.Config.MaxOpen[clientName]).getValue()
	maxOpenDefault := (DB.Config.MaxOpen["default"]).getValue()

	maxLifeTimeConns := time.Duration(configIntCoalesce(maxLifeTimeClient, maxLifeTimeDefault)) * time.Minute
	maxIdleConns := configIntCoalesce(maxIdleClient, maxIdleDefault)
	maxOpenConns := configIntCoalesce(maxOpenClient, maxOpenDefault)

	if !strings.Contains(strings.ToLower(host), "connect_timeout") {
		host = fmt.Sprintf("%s connect_timeout=%d", host, timeoutSec)
	}

	client, err := sqlx.Connect("postgres", host)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Failed to connect postgresql on host: %s, message: %s", host, err.Error()))
	}

	client.SetConnMaxLifetime(maxLifeTimeConns)
	client.SetMaxIdleConns(maxIdleConns)
	client.SetMaxOpenConns(maxOpenConns)

	return client, nil
}

func GetDB(clientName string, sRepl ...string) (*sqlx.DB, error) {
	defer SCIncrement(clientName)
	var conn *sqlx.DB

	if !DB.Config.Initialized {
		return nil, ErrNoConnection
	}

	repl := getReplication(sRepl)

	cli, exists := getHostMap(clientName)
	if !exists {
		log.Println("postgresql getconnection err", ErrNoConnection, clientName)
		return nil, ErrNoConnection
	}

	if !isActiveHost(clientName, repl) {
		return nil, ErrDisableConnection
	}

	if repl == "master" || len(cli.Slave) == 0 {
		conn = cli.Master
	} else {
		s := strings.Split(repl, "_")

		if s[0] != "slave" {
			return nil, ErrNoConnection
		}

		idx := 0
		if len(s) > 1 {
			slaveNum, err := strconv.Atoi(s[1])
			if err == nil {
				idx = slaveNum - 1
			}
		} else {
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			idx = r.Intn(len(cli.Slave))
		}

		if len(cli.Slave) <= idx {
			idx = len(cli.Slave) - 1
		}

		if idx < 0 {
			return nil, ErrNoConnection
		}

		// if available slave client greater than 1, slave connection will have postfix value. ex: slave_1
		// we need to check if random number generation slave connection still open
		if len(cli.Slave) > 0 {
			multiSlaveRepl := fmt.Sprintf("slave_%d", idx+1)
			if !isActiveHost(clientName, multiSlaveRepl) {
				return nil, ErrDisableConnection
			}
		}

		conn = cli.Slave[idx]
	}

	if conn == nil {
		return nil, ErrNoConnection
	}

	return conn, nil
}

// NewDatabaseClient returns new database client for a given client name along with all of its replications (master and slave).
// When flag reload is set, the current connection for specified replication names will be closed before new connection is made.
// If no replication names are supplied, the master and all of its slaves will be reloaded.
func NewDatabaseClient(clientName string, reload bool, repNames ...string) (DatabaseClientReplication, error) {
	GlobalLock.Lock()
	defer GlobalLock.Unlock()

	// register heartbeat
	defer RegisterHeartBeat(clientName, "master", 0)
	if lenSlaves := len(DB.Config.Connection[clientName].Slave); lenSlaves > 0 {
		defer RegisterHeartBeat(clientName, "slave", lenSlaves)
	}

	isRepReload := make(map[string]bool) // Marker for replication that needs to be reloaded.
	if reload {
		if cli, exists := DB.ClientList.Name[clientName]; exists {
			// Check if the replication need to be reloaded.
			// If so, close the connection and mark it.
			if len(repNames) == 0 || util.InArrayString("master", repNames) {
				cli.Master.Close()
				isRepReload["master"] = true
			} else {
				isRepReload["master"] = false
			}

			for i, conn := range cli.Slave {
				slaveName := fmt.Sprintf("slave_%d", i+1)
				if len(repNames) == 0 || util.InArrayString(slaveName, repNames) {
					conn.Close()
					isRepReload[slaveName] = true
				} else {
					isRepReload[slaveName] = false
				}
			}
		}
	}

	repl := DatabaseClientReplication{} // This will hold the new replications

	// Create/reuse connection
	// If flag reload is not set, create new connection for master and all of it slaves.
	// If it doesn't, create new replication that need to be reloaded.
	if !reload || isRepReload["master"] {
		// Master need to be reloaded, create new connection for it.
		hostMaster := DB.Config.Connection[clientName].Master
		replMaster, err := CreateConnection(clientName, hostMaster)
		if err != nil {
			return repl, err
		}
		repl.Master = replMaster
	} else {
		// Master doesn't need to be reloaded, reuse the connection.
		repl.Master = DB.ClientList.Name[clientName].Master
	}

	clientSlave := make([]*sqlx.DB, 0)
	for i, hostSlave := range DB.Config.Connection[clientName].Slave {
		slaveName := fmt.Sprintf("slave_%d", i+1)
		if !reload || isRepReload[slaveName] {
			// This slave need to be reloaded, so create connection for it.
			replSlave, err := CreateConnection(clientName, hostSlave)
			if err != nil {
				return repl, err
			}
			clientSlave = append(clientSlave, replSlave)
		} else {
			// Reuse the connection since it doesn't need to be reloaded.
			clientSlave = append(clientSlave, DB.ClientList.Name[clientName].Slave[i])
		}
	}
	repl.Slave = clientSlave

	DB.ClientList.Name[clientName] = repl

	return repl, nil
}

func NewDatabaseClientAll() error {
	if !DB.Config.Initialized {
		return errors.New("Config postgresql variable not initialized")
	}
	var errReturn error
	// Want all connections at least tried to be initialized before returning error
	for clientName := range DB.Config.Connection {
		_, err := NewDatabaseClient(clientName, false)
		if err != nil {
			log.Println("[Postgresql] db ", clientName, " Error creating client: ", err)
			errReturn = err
		}
	}

	return errReturn
}

//InitDatabaseConfig first reads config from filepath, then calls Vault to database credentials
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

func doPing(clientName, repl string) error {
	db, err := GetDB(clientName, repl)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	ch := make(chan int)
	go func(db *sqlx.DB) {
		var i int
		if err := db.QueryRow("select 1").Scan(&i); err != nil {
			select {
			case <-ctx.Done():
			case ch <- 0:
			}
			return
		}

		select {
		case <-ctx.Done():
		case ch <- i:
		}
	}(db)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case res := <-ch:
		if res == 1 {
			return nil
		}
	}

	// no clue what happen, still go to circuit breaker
	return errors.New("beat ping undefined error")
}

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

			errPing := doPing(clientName, repl)

			cb := cb_cmap.(*breaker.Breaker)
			res := cb.Run(func() error {
				return errPing
			})

			if res == breaker.ErrBreakerOpen {
				if isActiveHost(clientName, repl) && !isRecentReloaded(clientName, repl, float64(timeDelay)) {
					log.Println("[postgresql] circuit breaker opens", clientName, repl)
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
		log.Println("[postgresql] reload", clientName, repl)
		NewDatabaseClient(clientName, reload, repl)
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
