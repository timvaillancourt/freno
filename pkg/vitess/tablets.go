package vitess

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/github/freno/pkg/config"
	"vitess.io/vitess/go/vt/proto/topodata"
)

// Tablet represents information about a running instance of vttablet.
type Tablet struct {
	Alias         *topodata.TabletAlias `json:"alias,omitempty"`
	MysqlHostname string                `json:"mysql_hostname,omitempty"`
	MysqlPort     int32                 `json:"mysql_port,omitempty"`
	Type          topodata.TabletType   `json:"type,omitempty"`
}

// filterReplicaTablets parses a list of tablets, returning replica tablets only
func filterReplicaTablets(tablets []Tablet) (replicas []Tablet) {
	for _, tablet := range tablets {
		if tablet.Type == topodata.TabletType_REPLICA {
			replicas = append(replicas, tablet)
		}
	}
	return replicas
}

// GetKeyspaceTablets reads from vitess /api/keyspace/<keyspace>/tablets/[shard]
// and returns a list of tablets
func (c *Client) GetKeyspaceTablets(settings config.VitessConfigurationSettings) (tablets []Tablet, err error) {
	if settings.TimeoutSecs == 0 {
		c.httpClient.Timeout = defaultTimeout
	} else {
		c.httpClient.Timeout = time.Duration(settings.TimeoutSecs) * time.Second
	}

	url := fmt.Sprintf("%s/keyspace/%s/tablets/%s", constructAPIURL(settings), settings.Keyspace, settings.Shard)
	resp, err := c.httpClient.Get(url)
	if err != nil {
		return tablets, err
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return tablets, err
	}

	err = json.Unmarshal(body, &tablets)
	return filterReplicaTablets(tablets), err
}
