package tools

import (
	"flag"
	"fmt"
	"sort"
	"time"

	"github.com/gholt/brimtext"
)

type progressReport struct {
	Name      string
	Time      time.Time
	Pass      bool
	Errors    []string
	Processes map[string]*processReport
}

func (r *progressReport) Passed() bool {
	return r.Pass
}

func (r *progressReport) String() string {
	s := fmt.Sprintf(
		"[%s] %s\n",
		r.Time.Format("2006-01-02 15:04:05"),
		r.Name,
	)
	for _, e := range r.Errors {
		s += fmt.Sprintf("!! %s\n", e)
	}
	data := [][]string{{
		"Process",
		"Started",
		"Progress Update",
		"Progress",
		"Completed",
	}}
	var processes []string
	for process := range r.Processes {
		processes = append(processes, process)
	}
	sort.Strings(processes)
	for _, process := range processes {
		report := r.Processes[process]
		data = append(data, []string{
			process,
			fmt.Sprintf("%s %s ago", report.Started.Format("2006-01-02 15:04:05"), time.Since(report.Started).Truncate(time.Second)),
			fmt.Sprintf("%s %s ago", report.ProgressUpdated.Format("2006-01-02 15:04:05"), time.Since(report.ProgressUpdated).Truncate(time.Second)),
			report.Progress,
			fmt.Sprintf("%s took %s", report.Completed.Format("2006-01-02 15:04:05"), report.Completed.Sub(report.Started).Truncate(time.Second)),
		})
		if report.Started.IsZero() {
			data[len(data)-1][1] = ""
		}
		if report.ProgressUpdated.IsZero() {
			data[len(data)-1][2] = ""
		}
		if report.Completed.IsZero() {
			data[len(data)-1][4] = ""
		}
	}
	opts := brimtext.NewUnicodeBoxedAlignOptions()
	opts.Widths = []int{10, 10, 10, 23, 10}
	opts.Alignments = []brimtext.Alignment{brimtext.Right, brimtext.Right, brimtext.Right, brimtext.Left, brimtext.Right}
	s += brimtext.Align(data, opts)
	return s
}

type processReport struct {
	Started         time.Time
	ProgressUpdated time.Time
	Progress        string
	Completed       time.Time
}

func getProgressReport(flags *flag.FlagSet) *progressReport {
	report := &progressReport{
		Name:      "Progress Report",
		Time:      time.Now().UTC(),
		Pass:      true,
		Processes: map[string]*processReport{},
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
	ppds, err := db.processPasses()
	if err != nil {
		report.Errors = append(report.Errors, err.Error())
		return report
	}
	for _, ppd := range ppds {
		process := ppd.process
		if ppd.rtype != "" {
			if ppd.rtype == "object-overall" {
				process += " object"
			} else {
				process += " " + ppd.rtype
			}
		}
		if ppd.policy != 0 || ppd.rtype == "object" {
			process += fmt.Sprintf("-%d", ppd.policy)
		}
		report.Processes[process] = &processReport{
			Started:         ppd.startDate,
			ProgressUpdated: ppd.progressDate,
			Progress:        ppd.progress,
			Completed:       ppd.completeDate,
		}
	}
	return report
}
