package accountserver

import (
	"net/http"

	"github.com/troubling/hummingbird/common/ring"
)

type PriorityRepJob struct {
	Partition  uint64       `json:"partition"`
	FromDevice *ring.Device `json:"from_device"`
	ToDevice   *ring.Device `json:"to_device"`
}

// TODO
func SendPriRepJob(job *PriorityRepJob, client *http.Client) (string, bool) {
	return "pretending to do priority replication; normal replication should be fast enough for now", true
}
