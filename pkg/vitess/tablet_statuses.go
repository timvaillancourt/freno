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

type TabletHealth struct {
	Alias  *topodata.TabletAlias
	Health tabletHealthState
}

type TabletStatuses struct {
	Aliases [][]*topodata.TabletAlias
	Data    [][]tabletHealthState
}

func GetKeyspaceTabletStatuses(settings config.VitessConfigurationSettings) ([]*TabletHealth, error) {
	url := fmt.Sprintf("%s/tablet_statuses/?cell=all&keyspace=%s&metric=health&type=replica",
		constructAPIURL(settings),
		settings.Keyspace,
	)
	data, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer data.Body.Close()
	body, err := ioutil.ReadAll(data.Body)
	if err != nil {
		return nil, err
	}

	resp := make([]TabletStatuses, 0)
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}
	if len(resp) != 1 {
		return nil, err
	}

	statuses := resp[0]
	tablets := make([]*TabletHealth, 0)
	for i, alias := range statuses.Aliases {
		tablets = append(tablets, &TabletHealth{
			Alias:  alias[0],
			Health: statuses.Data[i][0],
		})
	}

	return tablets, err
}
