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

func TestGetKeyspaceTabletStatuses(t *testing.T) {
	vtctldApi := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.String() {
		case "/api/tablet_statuses/?cell=all&keyspace=test_ks&metric=health&type=replica":
			data := []*TabletStatuses{
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

	tabletStatuses, err := GetKeyspaceTabletStatuses(config.VitessConfigurationSettings{
		API:      vtctldApi.URL,
		Keyspace: "test_ks",
	})
	if err != nil {
		t.Fatalf("%v", err)
	}
	if len(tabletStatuses) != 2 {
		t.Fatal("expected only 2 tablets")
	}

	healthyTablet := tabletStatuses[0]
	if healthyTablet.Alias.Cell != "test" || healthyTablet.Alias.Uid != 123456 {
		t.Fatalf("expected tablet alias with cell='test' and uid=123456, got %v", healthyTablet.Alias)
	}
	if healthyTablet.Health != tabletHealthy {
		t.Fatal("expected healthy tablet")
	}

	degradedTablet := tabletStatuses[1]
	if degradedTablet.Health != tabletDegraded {
		t.Fatal("expected degraded tablet")
	}
}
