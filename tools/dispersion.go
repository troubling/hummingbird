package tools

import (
	"flag"
	"fmt"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/ring"
)

var partitionListCap = 4

type dispersionReport struct {
	Name            string
	Time            time.Time
	Pass            bool
	Errors          []string
	IssueCount      int
	ContainerReport *innerDispersionReport
	ObjectReports   map[int]*innerDispersionReport
}

func (r *dispersionReport) Passed() bool {
	return r.Pass
}

func (r *dispersionReport) String() string {
	s := fmt.Sprintf(
		"[%s] %s\n",
		r.Time.Format("2006-01-02 15:04:05"),
		r.Name,
	)
	for _, e := range r.Errors {
		s += fmt.Sprintf("!! %s\n", e)
	}
	if r.ContainerReport != nil {
		s += fmt.Sprintf("\nContainer Dispersion Report\n%s", r.ContainerReport.String())
	}
	var policies []int
	for policy := range r.ObjectReports {
		policies = append(policies, policy)
	}
	sort.Ints(policies)
	for policy := range policies {
		or := r.ObjectReports[policy]
		s += fmt.Sprintf("\nObject Dispersion Report for Policy: %d %s\n%s", policy, or.PolicyName, or.String())
	}
	return s
}

type innerDispersionReport struct {
	PolicyName      string
	Start           time.Time
	Complete        time.Time
	TotalPartitions int
	ReplicaCount    int
	Partitions      map[int][]*dispersionMissing
	ScanFailures    map[int][]*scanFailure
}

type scanFailure struct {
	Time     time.Time
	Service  string
	DeviceID int
}

func (r *innerDispersionReport) String() string {
	addPartitionListCap := func(s []string, p int) []string {
		if len(s) < partitionListCap {
			return append(s, fmt.Sprintf("%d", p))
		}
		if len(s) == partitionListCap {
			s[partitionListCap-1] = "..."
		}
		return s
	}
	var s string
	if r.Start.IsZero() {
		s += "    No known dispersion scan has been run.\n"
	} else if r.Complete.IsZero() {
		s += fmt.Sprintf("    Last dispersion scan started %s and has yet to complete after %s.\n", r.Start.Format("2006-01-02 15:04"), time.Since(r.Start).Truncate(time.Second))
	} else {
		s += fmt.Sprintf("    Last dispersion scan ran from %s to %s (%s ago for %s).\n", r.Start.Format("2006-01-02 15:04"), r.Complete.Format("2006-01-02 15:04"), time.Since(r.Complete).Truncate(time.Second), r.Complete.Sub(r.Start).Truncate(time.Second))
	}
	s += fmt.Sprintf("    There are %d partitions configured for %d copies.\n", r.TotalPartitions, r.ReplicaCount)
	if !r.Start.IsZero() && len(r.Partitions) == 0 {
		s += "    All partition copies were in place when last checked.\n"
	} else {
		var noCopies []string
		var oneCopies []string
		counts := map[int]int{}
		for partition, missings := range r.Partitions {
			switch r.ReplicaCount - len(missings) {
			case 0:
				noCopies = addPartitionListCap(noCopies, partition)
			case 1:
				oneCopies = addPartitionListCap(oneCopies, partition)
			}
			counts[len(missings)]++
		}
		var keys []int
		for key := range counts {
			keys = append(keys, key)
		}
		sort.Ints(keys)
		for _, key := range keys {
			if counts[key] == 1 {
				s += "    ! 1 partition was missing "
			} else {
				s += fmt.Sprintf("    ! %d partitions were missing ", counts[key])
			}
			if key == 1 {
				s += "1 copy"
			} else {
				s += fmt.Sprintf("%d copies", key)
			}
			if r.ReplicaCount-key == 0 {
				s += fmt.Sprintf("; NO COPIES AVAILABLE! %v\n", noCopies)
			} else if r.ReplicaCount-key == 1 {
				s += fmt.Sprintf("; DOWN TO ONE COPY! %v\n", oneCopies)
			} else {
				s += fmt.Sprintf("; %d copies found.\n", r.ReplicaCount-key)
			}
		}
	}
	failureCount := 0
	for _, sfs := range r.ScanFailures {
		failureCount += len(sfs)
	}
	if failureCount == 1 {
		s += "    !! 1 scan failure which may skew the report.\n"
	} else if failureCount > 1 {
		s += fmt.Sprintf("    !! %d scan failures which may skew the report.\n", failureCount)
	}
	if failureCount > 0 {
		s += fmt.Sprintf("    Assuming any scan failure is equivalent to a missing copy:\n")
		var noCopies []string
		var oneCopies []string
		counts := map[int]int{}
		for partition, missings := range r.Partitions {
			c := len(missings) + len(r.ScanFailures[partition])
			if c > r.ReplicaCount {
				c = r.ReplicaCount
			}
			switch r.ReplicaCount - c {
			case 0:
				if len(noCopies) <= partitionListCap {
					noCopies = append(noCopies, fmt.Sprintf("%d", partition))
				}
			case 1:
				if len(oneCopies) <= partitionListCap {
					oneCopies = append(oneCopies, fmt.Sprintf("%d", partition))
				}
			}
			counts[c]++
		}
		for partition, sfs := range r.ScanFailures {
			// Did we already count this in loop above?
			if r.Partitions[partition] == nil {
				c := len(sfs)
				if c > r.ReplicaCount {
					c = r.ReplicaCount
				}
				switch r.ReplicaCount - c {
				case 0:
					noCopies = addPartitionListCap(noCopies, partition)
				case 1:
					oneCopies = addPartitionListCap(oneCopies, partition)
				}
				counts[c]++
			}
		}
		var keys []int
		for key := range counts {
			keys = append(keys, key)
		}
		sort.Ints(keys)
		for _, key := range keys {
			if counts[key] == 1 {
				s += "        ! 1 partition was missing "
			} else {
				s += fmt.Sprintf("        ! %d partitions were missing ", counts[key])
			}
			if key == 1 {
				s += "1 copy"
			} else {
				s += fmt.Sprintf("%d copies", key)
			}
			if r.ReplicaCount-key == 0 {
				s += fmt.Sprintf("; NO COPIES AVAILABLE! %v\n", noCopies)
			} else if r.ReplicaCount-key == 1 {
				s += fmt.Sprintf("; DOWN TO ONE COPY! %v\n", oneCopies)
			} else {
				s += fmt.Sprintf("; %d copies found.\n", r.ReplicaCount-key)
			}
		}
	}
	return s
}

type dispersionMissing struct {
	Time    time.Time
	Service string
	Device  string
}

func getDispersionReport(flags *flag.FlagSet) *dispersionReport {
	report := &dispersionReport{
		Name:          "Dispersion Report",
		Time:          time.Now().UTC(),
		Pass:          true,
		ObjectReports: map[int]*innerDispersionReport{},
	}
	serverconf, err := getAndrewdConf(flags)
	if err != nil {
		report.Errors = append(report.Errors, err.Error())
		return report
	}
	db, err := newDB(serverconf, "")
	if err != nil {
		report.Errors = append(report.Errors, err.Error())
		return report
	}
	ring, _ := getRing("", "container", 0)
	report.ContainerReport = &innerDispersionReport{TotalPartitions: int(ring.PartitionCount()), ReplicaCount: int(ring.ReplicaCount()), Partitions: map[int][]*dispersionMissing{}, ScanFailures: map[int][]*scanFailure{}}
	var progress string
	report.ContainerReport.Start, _, progress, report.ContainerReport.Complete, err = db.processPass("dispersion scan", "container", 0)
	if err != nil {
		report.Errors = append(report.Errors, err.Error())
	}
	if strings.HasSuffix(progress, "-init does not yet exist") {
		report.ContainerReport.Start = time.Time{}
		report.ContainerReport.Complete = time.Time{}
	}
	if dsfs, err := db.dispersionScanFailures("container", 0); err != nil {
		report.Errors = append(report.Errors, err.Error())
	} else {
		for _, dsf := range dsfs {
			report.ContainerReport.ScanFailures[dsf.partition] = append(report.ContainerReport.ScanFailures[dsf.partition], &scanFailure{Time: dsf.time, Service: dsf.service, DeviceID: dsf.deviceID})
		}
	}
	if qrs, err := db.queuedReplications("container", 0, "dispersion"); err != nil {
		report.Errors = append(report.Errors, err.Error())
	} else {
		for _, qr := range qrs {
			dev := ring.AllDevices()[qr.toDeviceID]
			if !dev.Active() {
				continue
			}
			report.ContainerReport.Partitions[qr.partition] = append(report.ContainerReport.Partitions[qr.partition], &dispersionMissing{
				Time:    qr.created,
				Service: fmt.Sprintf("%s:%d", dev.Ip, dev.Port),
				Device:  dev.Device,
			})
			report.Pass = false
		}
	}
	if policies, err := conf.GetPolicies(); err != nil {
		report.Errors = append(report.Errors, err.Error())
	} else {
		for _, policy := range policies {
			if policy.Config["andrewd"] == "ignore" {
				continue
			}
			ring, _ := getRing("", "object", policy.Index)
			objectReport := &innerDispersionReport{TotalPartitions: int(ring.PartitionCount()), ReplicaCount: int(ring.ReplicaCount()), Partitions: map[int][]*dispersionMissing{}, ScanFailures: map[int][]*scanFailure{}}
			var progress string
			objectReport.Start, _, progress, objectReport.Complete, err = db.processPass("dispersion scan", "object", policy.Index)
			if err != nil {
				report.Errors = append(report.Errors, err.Error())
				continue
			}
			if strings.HasSuffix(progress, "-init does not yet exist") {
				objectReport.Start = time.Time{}
				objectReport.Complete = time.Time{}
			}
			if dsfs, err := db.dispersionScanFailures("object", policy.Index); err != nil {
				report.Errors = append(report.Errors, err.Error())
			} else {
				for _, dsf := range dsfs {
					objectReport.ScanFailures[dsf.partition] = append(objectReport.ScanFailures[dsf.partition], &scanFailure{Time: dsf.time, Service: dsf.service, DeviceID: dsf.deviceID})
				}
			}
			objectReport.PolicyName = policy.Name
			report.ObjectReports[policy.Index] = objectReport
			qrs, err := db.queuedReplications("object", policy.Index, "dispersion")
			if err != nil {
				report.Errors = append(report.Errors, err.Error())
				continue
			}
			for _, qr := range qrs {
				dev := ring.AllDevices()[qr.toDeviceID]
				if !dev.Active() {
					continue
				}
				objectReport.Partitions[qr.partition] = append(objectReport.Partitions[qr.partition], &dispersionMissing{
					Time:    qr.created,
					Service: fmt.Sprintf("%s:%d", dev.Ip, dev.Port),
					Device:  dev.Device,
				})
				report.Pass = false
			}
		}
	}
	if len(report.Errors) > 0 {
		report.Pass = false
	}
	return report
}

// generateDispersionNames will feed a name per partition into the names
// channel. The container should be an empty string if you wish to have
// container names generated. The prefix will be the start of every name. The
// cancel channel can be closed to abort the name generation. The concurrency
// value, if 0, will default to 1/4 the number of CPUs (cores).
// Usually, the container name generation is called with
// generateDispersionNames("", "disp-conts-", containerRing, containerNames, cancel, 0)
// and the object name generation is called with
// generateDispersionNames("disp-objs-<POLICYINDEX>", "", objectRing, objectNames, cancel, 0)
func generateDispersionNames(container, prefix string, r ring.Ring, names chan string, cancel chan struct{}, concurrency uint64) {
	// if looking for container names, send container=""
	defer close(names)
	var wg sync.WaitGroup
	if concurrency < 1 {
		concurrency = uint64(runtime.NumCPU()) / 4
		if concurrency < 1 {
			concurrency = 1
		}
	}
	for goroutine := uint64(0); goroutine < concurrency; goroutine++ {
		wg.Add(1)
		go func(funcGoroutine uint64) {
			defer wg.Done()
			for partition := funcGoroutine; true; partition += concurrency {
				select {
				case <-cancel:
					break
				default:
				}
				devs := r.GetNodes(partition)
				if devs == nil {
					break
				}
				for i := uint64(0); true; i++ {
					select {
					case <-cancel:
						break
					default:
					}
					c := container
					o := ""
					n := fmt.Sprintf("%s%d-%d", prefix, partition, i)
					if c == "" {
						c = n
					} else {
						o = n
					}
					genPart := r.GetPartition(AdminAccount, c, o)
					if genPart == partition {
						names <- n
						break
					}
				}
			}
		}(goroutine)
	}
	wg.Wait()
}
