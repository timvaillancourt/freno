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
	defaultTabletCacheExpiry = 3 * time.Minute
	defaultTimeout           = time.Duration(5) * time.Second
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

func NewClient() *Client {
	return &Client{
		httpClient: &http.Client{
			Timeout: defaultTimeout,
		},
		tabletCache: cache.New(defaultTabletCacheExpiry, time.Second),
	}
}

func (c *Client) GetHealthyReplicas(settings config.VitessConfigurationSettings) (tablets []*Tablet, err error) {
	statuses, err := c.getTabletStatuses(settings)
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
