package vitess

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

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

// tabletStatuses represents a response from a vtctld /api/tablet_statuses call
type tabletStatuses struct {
	Aliases [][]*topodata.TabletAlias
	Data    [][]tabletHealthState
}

// tabletStatus represents the status of a tablet
type tabletStatus struct {
	Alias  *topodata.TabletAlias
	Health tabletHealthState
}

// getReplicaTabletStatuses reads from vtctld /api/tablet_statuses/?<params...>
// and parses the result into a slice of tabletStatus structs
func (c *Client) getReplicaTabletStatuses(settings config.VitessConfigurationSettings) (tablets []*tabletStatus, err error) {
	cells := ParseCells(settings)
	if len(cells) == 0 {
		cells = []string{"all"}
	}
	for _, cell := range cells {
		url := fmt.Sprintf("%s/tablet_statuses/?cell=%s&keyspace=%s&metric=health&type=replica",
			constructAPIURL(settings),
			cell,
			settings.Keyspace,
		)
		resp, err := c.getHTTPClient(settings).Get(url)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("%v", resp.Status)
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
		for cellIdx, cellAliases := range statuses.Aliases {
			for tabletIdx, cellAlias := range cellAliases {
				tablets = append(tablets, &tabletStatus{
					Alias:  cellAlias,
					Health: statuses.Data[cellIdx][tabletIdx],
				})
			}
		}
	}
	if len(tablets) < 1 {
		return nil, fmt.Errorf("found no tablets")
	}

	return tablets, err
}
