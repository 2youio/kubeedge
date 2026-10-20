package dao

import (
	"path/filepath"
	"testing"

	"github.com/beego/beego/v2/client/orm"

	"github.com/kubeedge/kubeedge/edge/pkg/common/dbm"
)

// TestDeleteMetaByKeyAndUID runs the conditional delete against a real sqlite database.
func TestDeleteMetaByKeyAndUID(t *testing.T) {
	orm.RegisterModel(new(Meta))
	dbm.InitDBConfig("sqlite3", "default", filepath.Join(t.TempDir(), "edgecore.db"))
	dbm.DBAccess = orm.NewOrmUsingDB("default")

	tests := []struct {
		name    string
		value   string
		deleted bool
	}{
		{"same uid", `{"metadata":{"name":"cm","uid":"uid-a"}}`, true},
		{"object recreated with another uid", `{"metadata":{"name":"cm","uid":"uid-b"}}`, false},
		{"row without uid", `{"metadata":{"name":"cm"}}`, true},
		{"row that is not json", `not json`, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := InsertOrUpdate(&Meta{Key: "default/configmap/cm", Type: "configmap", Value: test.value}); err != nil {
				t.Fatal(err)
			}
			num, err := DeleteMetaByKeyAndUID("default/configmap/cm", "uid-a")
			if err != nil {
				t.Fatal(err)
			}
			if got := num == 1; got != test.deleted {
				t.Errorf("deleted = %v, want %v", got, test.deleted)
			}
			_ = DeleteMetaByKey("default/configmap/cm")
		})
	}
}
