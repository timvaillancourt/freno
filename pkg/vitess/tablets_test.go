package vitess

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/github/freno/pkg/config"
	"vitess.io/vitess/go/vt/proto/topodata"
)

func TestGetKeyspaceTablets(t *testing.T) {
	c := NewClient()
	vtctldApi := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.String() {
		case "/api/keyspace/test/tablets/00":
			data, _ := json.Marshal([]Tablet{
				{
					MysqlHostname: "master",
					Type:          topodata.TabletType_MASTER,
				},
				{
					MysqlHostname: "replica",
					Type:          topodata.TabletType_REPLICA,
				},
				{
					MysqlHostname: "spare",
					Type:          topodata.TabletType_SPARE,
				},
				{
					MysqlHostname: "batch",
					Type:          topodata.TabletType_BATCH,
				},
				{
					MysqlHostname: "backup",
					Type:          topodata.TabletType_BACKUP,
				},
				{

					MysqlHostname: "restore",
					Type:          topodata.TabletType_RESTORE,
				},
			})
			fmt.Fprint(w, string(data))
		default:
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, "[]")
		}
	}))
	defer vtctldApi.Close()

	t.Run("success", func(t *testing.T) {
		tablets, err := c.GetKeyspaceTablets(config.VitessConfigurationSettings{
			API:         vtctldApi.URL,
			Keyspace:    "test",
			Shard:       "00",
			TimeoutSecs: 1,
		})
		if err != nil {
			t.Fatalf("Expected no error, got %q", err)
		}

		if len(tablets) != 1 {
			t.Fatalf("Expected 1 tablet, got %d", len(tablets))
		}

		if tablets[0].MysqlHostname != "replica" {
			t.Fatalf("Expected hostname %q, got %q", "replica", tablets[0].MysqlHostname)
		}

		if c.httpClient.Timeout != time.Second {
			t.Fatalf("Expected vitess client timeout of %v, got %v", time.Second, c.httpClient.Timeout)
		}
	})

	t.Run("not-found", func(t *testing.T) {
		tablets, err := c.GetKeyspaceTablets(config.VitessConfigurationSettings{
			API:      vtctldApi.URL,
			Keyspace: "not-found",
			Shard:    "40-80",
		})
		if err != nil {
			t.Fatalf("Expected no error, got %q", err)
		}

		if len(tablets) > 0 {
			t.Fatalf("Expected 0 tablets, got %d", len(tablets))
		}

		if c.httpClient.Timeout != defaultTimeout {
			t.Fatalf("Expected vitess client timeout of %v, got %v", defaultTimeout, c.httpClient.Timeout)
		}
	})

	t.Run("failed", func(t *testing.T) {
		vtctldApi.Close() // kill the mock vitess API
		_, err := c.GetKeyspaceTablets(config.VitessConfigurationSettings{
			API:      vtctldApi.URL,
			Keyspace: "fail",
			Shard:    "00",
		})
		if err == nil {
			t.Fatal("Expected error, got nil")
		}
	})
}
