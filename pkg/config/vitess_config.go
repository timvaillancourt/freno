package config

//
// Vitess-specific configuration
//

type VitessConfigurationSettings struct {
	API                string
	Cells              []string
	Keyspace           string
	Shard              string
	TabletCacheTTLSecs int
	TimeoutSecs        uint
	RealtimeStats      bool
}

func (settings *VitessConfigurationSettings) IsEmpty() bool {
	if settings.API == "" {
		return true
	}
	if settings.Keyspace == "" {
		return true
	}
	return false
}
