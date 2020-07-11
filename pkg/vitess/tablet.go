package vitess

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/github/freno/pkg/config"
	"github.com/patrickmn/go-cache"
	"vitess.io/vitess/go/vt/proto/topodata"
)

// Tablet represents information about a running instance of vttablet.
type Tablet struct {
	MysqlHostname string `json:"mysql_hostname,omitempty"`
	MysqlPort     int32  `json:"mysql_port,omitempty"`
}

func tabletCacheKey(tabletAlias *topodata.TabletAlias) string {
	return fmt.Sprintf("%s-%d", tabletAlias.Cell, tabletAlias.Uid)
}

func (c *Client) cacheTablet(settings config.VitessConfigurationSettings, tabletAlias *topodata.TabletAlias, tablet *Tablet) {
	ttl := cache.DefaultExpiration
	if settings.TabletCacheTTLSecs > 0 {
		ttl = time.Duration(settings.TabletCacheTTLSecs) * time.Second
	}
	c.tabletCache.Set(tabletCacheKey(tabletAlias), tablet, ttl)
}

// GetTablet reads from vitess /api/tablets/<tabletAlias> and returns a Tablet struct
func (c *Client) getTablet(settings config.VitessConfigurationSettings, tabletAlias *topodata.TabletAlias) (tablet *Tablet, err error) {
	if tablet, found := c.tabletCache.Get(tabletCacheKey(tabletAlias)); found {
		return tablet.(*Tablet), nil
	}

	if settings.TimeoutSecs == 0 {
		c.httpClient.Timeout = defaultTimeout
	} else {
		c.httpClient.Timeout = time.Duration(settings.TimeoutSecs) * time.Second
	}

	url := fmt.Sprintf("%s/tablets/%s-%d", constructAPIURL(settings), tabletAlias.Cell, tabletAlias.Uid)
	resp, err := c.httpClient.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		err = json.Unmarshal(body, &tablet)
		if err == nil {
			c.cacheTablet(settings, tabletAlias, tablet)
		}
	}

	return tablet, err
}
