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

func TestGetTablet(t *testing.T) {
	c := NewClient()
	vtctldApi := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.String() {
		case "/api/tablets/test-123456":
			data, _ := json.Marshal(Tablet{
				MysqlHostname: "replica1",
				MysqlPort:     3306,
			})
			fmt.Fprint(w, string(data))
		default:
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, "{}")
		}
	}))
	defer vtctldApi.Close()

	settings := config.VitessConfigurationSettings{
		API: vtctldApi.URL,
	}
	t.Run("success", func(t *testing.T) {
		settings.TimeoutSecs = 1
		tabletAlias := &topodata.TabletAlias{Cell: "test", Uid: 123456}
		tablet, err := c.GetTablet(settings, tabletAlias)
		if err != nil {
			t.Fatalf("Expected no error, got %q", err)
		}

		if tablet.MysqlHostname != "replica1" {
			t.Fatalf("Expected hostname %q, got %q", "replica", tablet.MysqlHostname)
		}

		if c.httpClient.Timeout != time.Second {
			t.Fatalf("Expected vitess client timeout of %v, got %v", time.Second, c.httpClient.Timeout)
		}

		if _, found := c.tabletCache.Get(tabletCacheKey(tabletAlias)); !found {
			t.Fatalf("expected key %v in cache", tabletCacheKey(tabletAlias))
		}
	})

	t.Run("not-found", func(t *testing.T) {
		settings.TimeoutSecs = 0
		tabletAlias := &topodata.TabletAlias{Cell: "test", Uid: 1}
		tablet, err := c.GetTablet(settings, tabletAlias)
		if err != nil {
			t.Fatalf("Expected no error, got %q", err)
		}

		if tablet != nil {
			t.Fatalf("expected nil tablet, got %v", tablet)
		}

		if c.httpClient.Timeout != defaultTimeout {
			t.Fatalf("Expected vitess client timeout of %v, got %v", defaultTimeout, c.httpClient.Timeout)
		}

		if _, found := c.tabletCache.Get(tabletCacheKey(tabletAlias)); found {
			t.Fatalf("expected key %v to be absent in cache", tabletCacheKey(tabletAlias))
		}
	})

	t.Run("failed", func(t *testing.T) {
		vtctldApi.Close() // kill the mock vitess API
		_, err := c.GetTablet(settings, &topodata.TabletAlias{Cell: "test", Uid: 123})
		if err == nil {
			t.Fatal("Expected error, got nil")
		}
	})
}
