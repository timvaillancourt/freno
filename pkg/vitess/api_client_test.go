package vitess

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/github/freno/pkg/config"
	"vitess.io/vitess/go/vt/proto/topodata"
)

func TestGetHealthyReplicaTablets(t *testing.T) {
	vtctldApi := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.String() {
		case "/api/tablets/test-123456":
			bytes, _ := json.Marshal(Tablet{
				MysqlHostname: "test-replica",
				MysqlPort:     3306,
			})
			fmt.Fprint(w, string(bytes))
		case "/api/tablet_statuses/?cell=all&keyspace=test_ks&metric=health&type=replica":
			data := []*tabletStatuses{
				{
					Aliases: [][]*topodata.TabletAlias{
						{
							{Cell: "test", Uid: 123456},
						},
						{
							{Cell: "test", Uid: 123457},
						},
					},
					Data: [][]tabletHealthState{
						{
							tabletHealthy,
						},
						{
							tabletDegraded,
						},
					},
				},
			}
			bytes, _ := json.Marshal(data)
			fmt.Fprint(w, string(bytes))
		default:
			t.Fatalf("unexpected vtctld API call: %q", r.URL.String())
		}
	}))
	defer vtctldApi.Close()

	c := NewClient(config.MySQLConfigurationSettings{})
	tablets, err := c.GetHealthyReplicaTablets(config.VitessConfigurationSettings{
		API:      vtctldApi.URL,
		Keyspace: "test_ks",
	})
	if err != nil {
		t.Fatalf("%v", err)
	}

	if len(tablets) != 1 {
		t.Fatalf("expected only 1 healthy tablet, got %d", len(tablets))
	}

	tablet := tablets[0]
	if tablet.MysqlHostname != "test-replica" || tablet.MysqlPort != 3306 {
		t.Fatalf("expected hostname/port 'test-replica:3306', got %v", tablet)
	}
}
