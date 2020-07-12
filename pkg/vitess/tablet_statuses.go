package vitess

import (
	"encoding/json"
	"fmt"
	"io/ioutil"

	"github.com/github/freno/pkg/config"
	"vitess.io/vitess/go/vt/proto/topodata"
)

// tabletHealthState is a copy of the tablet health consts from the vtctld source:
// https://github.com/vitessio/vitess/blob/master/go/vt/vtctld/tablet_stats_cache.go#L30-L40
type tabletHealthState int

const (
	tabletHealthy   tabletHealthState = 0
	tabletDegraded  tabletHealthState = 1
	tabletUnhealthy tabletHealthState = 2
)

type tabletStatuses struct {
	Aliases [][]*topodata.TabletAlias
	Data    [][]tabletHealthState
}

type tabletStatus struct {
	Alias  *topodata.TabletAlias
	Health tabletHealthState
}

func (c *Client) getReplicaTabletStatuses(settings config.VitessConfigurationSettings) ([]*tabletStatus, error) {
	url := fmt.Sprintf("%s/tablet_statuses/?cell=all&keyspace=%s&metric=health&type=replica",
		constructAPIURL(settings),
		settings.Keyspace,
	)
	resp, err := c.getHTTPClient(settings).Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, err
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	statusesSlice := make([]tabletStatuses, 0)
	if err := json.Unmarshal(body, &statusesSlice); err != nil {
		return nil, err
	}
	if len(statusesSlice) != 1 {
		return nil, err
	}

	statuses := statusesSlice[0]
	tablets := make([]*tabletStatus, 0)
	for i, alias := range statuses.Aliases {
		tablets = append(tablets, &tabletStatus{
			Alias:  alias[0],
			Health: statuses.Data[i][0],
		})
	}

	return tablets, err
}
