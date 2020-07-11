package vitess

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/github/freno/pkg/config"
)

const defaultTimeout = time.Duration(5) * time.Second

func constructAPIURL(settings config.VitessConfigurationSettings) (url string) {
	api := strings.TrimRight(settings.API, "/")
	if !strings.HasSuffix(api, "/api") {
		api = fmt.Sprintf("%s/api", api)
	}
	return api
}

type Client struct {
	httpClient *http.Client
}

func NewClient() *Client {
	return &Client{
		httpClient: &http.Client{
			Timeout: defaultTimeout,
		},
	}
}

func (c *Client) GetHealthyReplicas(settings config.VitessConfigurationSettings) (tablets []Tablet, err error) {
	return c.GetKeyspaceTablets(settings)
}
