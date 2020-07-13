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

// constructAPIURL returns a string of the base URL of the vtctld API
func constructAPIURL(settings config.VitessConfigurationSettings) (url string) {
	api := strings.TrimRight(settings.API, "/")
	if !strings.HasSuffix(api, "/api") {
		api = fmt.Sprintf("%s/api", api)
	}
	return api
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
