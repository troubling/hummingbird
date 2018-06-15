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
		"Ago",
		"Status",
		"Detail",
	}}
	var processes []string
	for process := range r.Processes {
		processes = append(processes, process)
	}
	sort.Strings(processes)
	for _, process := range processes {
		report := r.Processes[process]
		var ago string
		var status string
		var detail string
		if report.Started.IsZero() {
			status = "Never Run"
		} else if report.Completed.IsZero() {
			ago = time.Since(report.Started).Truncate(time.Second).String()
			status = "Running"
			if report.Progress != "" {
				detail = fmt.Sprintf("%s ago: %s", time.Since(report.ProgressUpdated).Truncate(time.Second), report.Progress)
			}
		} else {
			ago = time.Since(report.Completed).Truncate(time.Second).String()
			status = "Completed"
			detail = report.Progress
		}
		data = append(data, nil)
		data = append(data, []string{process, ago, status, detail})
		if report.PreviousProgress != "" {
			if report.PreviousCompleted.IsZero() {
				data = append(data, []string{"", "", "", fmt.Sprintf("Previous pass did not complete: %s", report.PreviousProgress)})
			} else {
				data = append(data, []string{"", "", "", fmt.Sprintf("Previous pass completed %s ago: %s", time.Since(report.PreviousCompleted).Truncate(time.Second), report.PreviousProgress)})
			}
		}
	}
	opts := brimtext.NewUnicodeBoxedAlignOptions()
	opts.NilBetweenEveryRow = false
	opts.Alignments = []brimtext.Alignment{brimtext.Right, brimtext.Center, brimtext.Left, brimtext.Left}
	opts.Widths = []int{30, 7, 10}
	w := brimtext.GetTTYWidth() - 5 // 2 for the leading `║ `, two for the trailing ` ║`, and 1 more for terminals that wrap weird right at limit
	for _, x := range opts.Widths {
		w -= x + 3 // 3 for the separating ` │ `
	}
	if w < 0 {
		w = 0
	}
	opts.Widths = append(opts.Widths, w)
	s += brimtext.Align(data, opts)
	return s
}

type processReport struct {
	Started           time.Time
	ProgressUpdated   time.Time
	Progress          string
	Completed         time.Time
	PreviousProgress  string
	PreviousCompleted time.Time
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
			Started:           ppd.startDate,
			ProgressUpdated:   ppd.progressDate,
			Progress:          ppd.progress,
			Completed:         ppd.completeDate,
			PreviousProgress:  ppd.previousProgress,
			PreviousCompleted: ppd.previousCompleteDate,
		}
	}
	return report
}
