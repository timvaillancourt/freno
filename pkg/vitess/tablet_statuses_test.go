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

func TestGetReplicaTabletStatuses(t *testing.T) {
	vtctldApi := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.String() {
		case "/api/tablet_statuses/?cell=all&keyspace=test_ks&metric=health&type=replica":
			data := []*tabletStatuses{
				{
					Aliases: [][]*topodata.TabletAlias{
						{
							{Cell: "ac4", Uid: 123456},
							{Cell: "ac4", Uid: 123457},
						},
						{
							{Cell: "va3", Uid: 123458},
						},
					},
					Data: [][]tabletHealthState{
						{
							tabletHealthy,
							tabletDegraded,
						},
						{
							tabletHealthy,
						},
					},
				},
			}
			bytes, _ := json.Marshal(data)
			fmt.Fprint(w, string(bytes))
		case "/api/tablet_statuses/?cell=all&keyspace=not-found&metric=health&type=replica":
			w.WriteHeader(http.StatusOK)  // this API returns a 200 on a keyspace that does not exist
			data := []*tabletStatuses{{}} // and it returns and empty tablet status
			bytes, _ := json.Marshal(data)
			fmt.Fprint(w, string(bytes))
		default:
			t.Fatalf("unexpected vtctld API call: %q", r.URL.String())
		}
	}))
	defer vtctldApi.Close()

	c := NewClient(config.MySQLConfigurationSettings{})

	t.Run("success", func(t *testing.T) {
		statuses, err := c.getReplicaTabletStatuses(config.VitessConfigurationSettings{
			API:      vtctldApi.URL,
			Keyspace: "test_ks",
		})
		if err != nil {
			t.Fatalf("%v", err)
		}
		if len(statuses) != 3 {
			t.Fatal("expected only 3 tablets")
		}

		healthyTablet := statuses[0]
		if healthyTablet.Alias.Cell != "ac4" || healthyTablet.Alias.Uid != 123456 {
			t.Fatalf("expected tablet alias with cell='ac4' and uid=123456, got %v", healthyTablet.Alias)
		}
		if healthyTablet.Health != tabletHealthy {
			t.Fatal("expected healthy tablet")
		}

		degradedTablet := statuses[1]
		if degradedTablet.Health != tabletDegraded {
			t.Fatal("expected degraded tablet")
		}

		otherHealthyTablet := statuses[2]
		if otherHealthyTablet.Alias.Cell != "va3" || otherHealthyTablet.Alias.Uid != 123458 {
			t.Fatalf("expected tablet alias with cell='va3' and uid=123458, got %v", otherHealthyTablet.Alias)
		}
		if otherHealthyTablet.Health != tabletHealthy {
			t.Fatal("expected degraded tablet")
		}
	})

	t.Run("not-found", func(t *testing.T) {
		statuses, err := c.getReplicaTabletStatuses(config.VitessConfigurationSettings{
			API:      vtctldApi.URL,
			Keyspace: "not-found",
		})
		if err == nil {
			t.Fatal("expected an error, got nil")
		}
		if len(statuses) != 0 {
			t.Fatal("expected 0 statuses")
		}
	})

	t.Run("failed", func(t *testing.T) {
		vtctldApi.Close() // kill the mock vitess API
		_, err := c.getReplicaTabletStatuses(config.VitessConfigurationSettings{
			API: vtctldApi.URL,
		})
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})
}
