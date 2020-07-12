package vitess

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/github/freno/pkg/config"
	"github.com/patrickmn/go-cache"
)

const (
	defaultTabletCacheTTL = time.Duration(10) * time.Minute
	defaultTimeout        = time.Duration(5) * time.Second
)

func constructAPIURL(settings config.VitessConfigurationSettings) (url string) {
	api := strings.TrimRight(settings.API, "/")
	if !strings.HasSuffix(api, "/api") {
		api = fmt.Sprintf("%s/api", api)
	}
	return api
}

type Client struct {
	httpClient  *http.Client
	tabletCache *cache.Cache
}

func NewClient(settings config.MySQLConfigurationSettings) *Client {
	defaultTTL := defaultTabletCacheTTL
	if settings.VitessTabletCacheTTLSecs > 0 {
		defaultTTL = time.Duration(settings.VitessTabletCacheTTLSecs) * time.Second
	}
	return &Client{
		httpClient: &http.Client{
			Timeout: defaultTimeout,
		},
		tabletCache: cache.New(defaultTTL, time.Second),
	}
}

func (c *Client) setClientTimeout(settings config.VitessConfigurationSettings) {
	if settings.TimeoutSecs == 0 {
		c.httpClient.Timeout = defaultTimeout
	} else {
		c.httpClient.Timeout = time.Duration(settings.TimeoutSecs) * time.Second
	}
}

func (c *Client) GetHealthyReplicaTablets(settings config.VitessConfigurationSettings) (tablets []*Tablet, err error) {
	statuses, err := c.getReplicaTabletStatuses(settings)
	if err != nil {
		return tablets, err
	}
	for _, status := range statuses {
		if status.Health != tabletHealthy {
			continue
		}
		tablet, err := c.getTablet(settings, status.Alias)
		if err == nil {
			tablets = append(tablets, tablet)
		}
	}
	return tablets, err
}
