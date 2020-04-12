package postgresql

import (
	"errors"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
)

type (
	MockDatabaseClientList struct {
		Name    map[string]MockDatabaseClientReplication
		Runtime map[string]MockRuntimeSettingRepl
	}

	MockDatabaseClientReplication struct {
		Master sqlmock.Sqlmock
		Slave  sqlmock.Sqlmock
	}

	MockRuntimeSettingRepl map[string]*MockRuntimeSetting

	MockRuntimeSetting struct {
		Active     bool
		LastReload time.Time
	}

	MockConnectionx struct {
		Conn sqlmock.Sqlmock
	}
)

const (
	MOCK_NO    = 0
	MOCK_DATA  = 1
	MOCK_ERROR = 2

	ERR_DB_NOT_FOUND = "database not found"
)

var (
	MockDBClientList = &MockDatabaseClientList{Name: make(map[string]MockDatabaseClientReplication)}
)

func CreateConnectionMock(clientName string, host string, driverName string) (*sqlx.DB, error) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Unable to create new mock database %s for host [%s], message: %s", driverName, host, err.Error()))
	}

	mockSqlxDB := sqlx.NewDb(mockDB, driverName)

	repl := MockDBClientList.Name[clientName]

	if host == "master_mock" {
		repl.Master = mock
		MockDBClientList.Name[clientName] = repl
	} else if host == "slave_mock" {
		repl.Slave = mock
		MockDBClientList.Name[clientName] = repl
	}

	return mockSqlxDB, err
}
