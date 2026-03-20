package simulator

import "github.com/mobiletoly/go-oversync/oversqlite"

func managedSyncTables() []oversqlite.SyncTable {
	return []oversqlite.SyncTable{
		{TableName: "users", SyncKeyColumnName: "id"},
		{TableName: "posts", SyncKeyColumnName: "id"},
		{TableName: "categories", SyncKeyColumnName: "id"},
		{TableName: "teams", SyncKeyColumnName: "id"},
		{TableName: "team_members", SyncKeyColumnName: "id"},
		{TableName: "files", SyncKeyColumnName: "id"},
		{TableName: "file_reviews", SyncKeyColumnName: "id"},
	}
}
