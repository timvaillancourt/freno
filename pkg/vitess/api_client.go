package vitess

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/github/freno/pkg/config"
	"github.com/outbrain/golib/log"
	"github.com/patrickmn/go-cache"
)

const (
	defaultTabletCacheTTL = time.Duration(10) * time.Minute
	defaultTimeout        = time.Duration(5) * time.Second
)

// Tablet represents information about a running instance of vttablet.
type Tablet struct {
	Alias         *topodata.TabletAlias `json:"alias,omitempty"`
	MysqlHostname string                `json:"mysql_hostname,omitempty"`
	MysqlPort     int32                 `json:"mysql_port,omitempty"`
	Type          topodata.TabletType   `json:"type,omitempty"`
}

// HasValidCell returns a bool reflecting if a tablet is in a valid Vitess cell
func (t Tablet) HasValidCell(validCells []string) bool {
	if len(validCells) == 0 {
		return true
	}
	for _, cell := range validCells {
		if t.Alias.GetCell() == cell {
			return true
		}
	}
	return false
}

// IsValidReplica returns a bool reflecting if a tablet type is REPLICA
func (t Tablet) IsValidReplica() bool {
	return t.Type == topodata.TabletType_REPLICA
}

var httpClient = http.Client{
	Timeout: defaultTimeout,
}

// constructAPIURL returns a string of the base URL of the vtctld API
func constructAPIURL(settings config.VitessConfigurationSettings) (url string) {
	api := strings.TrimRight(settings.API, "/")
	if !strings.HasSuffix(api, "/api") {
		api = fmt.Sprintf("%s/api", api)
	}
	return api
}

// ParseCells returns a slice of non-empty Vitess cell names
func ParseCells(settings config.VitessConfigurationSettings) (cells []string) {
	for _, cell := range settings.Cells {
		cell = strings.TrimSpace(cell)
		if cell != "" {
			cells = append(cells, cell)
		}
	}
	return cells
}

// Client is a client for the vtctld API
type Client struct {
	httpClient  *http.Client
	tabletCache *cache.Cache
}

// NewClient returns a client for the vtctld API
func NewClient(settings config.MySQLConfigurationSettings) *Client {
	defaultTabletCacheTTL := defaultTabletCacheTTL
	if settings.VitessTabletCacheTTLSecs > 0 {
		defaultTabletCacheTTL = time.Duration(settings.VitessTabletCacheTTLSecs) * time.Second
	}
	return &Client{
		httpClient: &http.Client{
			Timeout: defaultTimeout,
		},
		tabletCache: cache.New(defaultTabletCacheTTL, time.Second),
	}
}

// getHTTPClient returns an HTTP client for the vtctld API
func (c *Client) getHTTPClient(settings config.VitessConfigurationSettings) *http.Client {
	if settings.TimeoutSecs == 0 {
		c.httpClient.Timeout = defaultTimeout
	} else {
		c.httpClient.Timeout = time.Duration(settings.TimeoutSecs) * time.Second
	}
	return c.httpClient
}

// GetHealthyReplicaTablets returns a slice of healthy vttables using the vtctld API
func (c *Client) GetHealthyReplicaTablets(settings config.VitessConfigurationSettings) (tablets []*Tablet, err error) {
	statuses, err := c.getReplicaTabletStatuses(settings)
	if err != nil {
		return tablets, err
	}

	var wg sync.WaitGroup
	tabletsChan := make(chan *Tablet, len(statuses))
	for _, status := range statuses {
		wg.Add(1)
		go func(settings config.VitessConfigurationSettings, wg *sync.WaitGroup, status *tabletStatus) {
			defer wg.Done()
			if status.Health != tabletHealthy {
				return
			}
			tablet, err := c.getTablet(settings, status.Alias)
			if err != nil {
				log.Errorf("Unable to get tablet alias '%s-%d' from vtctld API: %v", status.Alias.Cell, status.Alias.Uid, err)
			}
			tabletsChan <- tablet
		}(settings, &wg, status)
	}
	wg.Wait()
	close(tabletsChan)

	for tablet := range tabletsChan {
		if tablet != nil {
			tablets = append(tablets, tablet)
		}
	}
	return tablets, err
}
