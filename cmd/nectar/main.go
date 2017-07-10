package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"sort"
	"strings"

	"github.com/gholt/brimtext"
	"github.com/troubling/hummingbird/client"
)

var (
	globalFlags               = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	globalFlagAuthURL         = globalFlags.String("A", "", "|<url>| URL to auth system, example: http://127.0.0.1:8080/auth/v1.0")
	globalFlagAuthTenant      = globalFlags.String("T", "", "|<tenant>| Tenant name for auth system, example: test - Not all auth systems need this")
	globalFlagAuthUser        = globalFlags.String("U", "", "|<user>| User name for auth system, example: tester - Some auth systems allow tenant:user format here, example: test:tester")
	globalFlagAuthKey         = globalFlags.String("K", "", "|<key>| Key for auth system, example: testing - Some auth systems use passwords instead, see -P")
	globalFlagAuthPassword    = globalFlags.String("P", "", "|<password>| Password for auth system, example: testing - Some auth system use keys instead, see -K")
	globalFlagStorageRegion   = globalFlags.String("R", "", "|<region>| Storage region to use if set, otherwise uses the default")
	globalFlagInternalStorage = globalFlags.Bool("I", false, "Internal storage URL resolution, such as Rackspace ServiceNet")
	globalFlagHeaders         = stringListFlag{} // defined in init()
)

var (
	getFlags         = flag.NewFlagSet("get", flag.ExitOnError)
	getFlagRaw       = getFlags.Bool("r", false, "Emit raw results")
	getFlagNameOnly  = getFlags.Bool("n", false, "In listings, emits the names only")
	getFlagMarker    = getFlags.String("marker", "", "|<text>| In listings, sets the start marker")
	getFlagEndMarker = getFlags.String("endmarker", "", "|<text>| In listings, sets the stop marker")
	getFlagReverse   = getFlags.Bool("reverse", false, "In listings, reverses the order")
	getFlagLimit     = getFlags.Int("limit", 0, "|<number>| In listings, limits the results")
	getFlagPrefix    = getFlags.String("prefix", "", "|<text>|In listings, returns only those matching the prefix")
	getFlagDelimiter = getFlags.String("delimiter", "", "|<text>| In listings, sets the delimiter and activates delimiter listings")
)

var (
	headFlags   = flag.NewFlagSet("head", flag.ExitOnError)
	headFlagRaw = headFlags.Bool("r", false, "Emit raw headers")
)

func init() {
	globalFlags.Var(&globalFlagHeaders, "H", "|<name>:[value]| Sets a header to be sent with the request. Useful mostly for PUTs and POSTs, allowing you to set metadata. This option can be specified multiple times for additional headers.")
	var flagbuf bytes.Buffer
	globalFlags.SetOutput(&flagbuf)
	getFlags.SetOutput(&flagbuf)
	headFlags.SetOutput(&flagbuf)
}

func help(flags *flag.FlagSet, err error) {
	if err == flag.ErrHelp || err == nil {
		fmt.Fprintln(os.Stderr, os.Args[0], `[options] <subcommand> ...`)
		fmt.Fprintln(os.Stderr, brimtext.Wrap(`
Tool for accessing a Hummingbird/Swift cluster. The following global options are available:
        `, 0, "  ", "  "))
		helpFlags(globalFlags)
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, brimtext.Wrap(`
The following subcommands are available:
        `, 0, "", ""))
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "delete [container] [object]")
		fmt.Fprintln(os.Stderr, brimtext.Wrap(`
Performs a DELETE request. A DELETE, as probably expected, is used to remove the target.
        `, 0, "  ", "  "))
		fmt.Fprintln(os.Stderr, "get [options] [container] [object]")
		fmt.Fprintln(os.Stderr, brimtext.Wrap(`
Performs a GET request. A GET on an account or container will output the listing of containers or objects, respectively. A GET on an object will output the content of the object to standard output.
        `, 0, "  ", "  "))
		helpFlags(getFlags)
		fmt.Fprintln(os.Stderr, "\nhead [options] [container] [object]")
		fmt.Fprintln(os.Stderr, brimtext.Wrap(`
Performs a HEAD request, giving overall information about the account, container, or object.
        `, 0, "  ", "  "))
		helpFlags(headFlags)
		fmt.Fprintln(os.Stderr, "\npost [container] [object]")
		fmt.Fprintln(os.Stderr, brimtext.Wrap(`
Performs a POST request. POSTs allow you to update the metadata for the target.
        `, 0, "  ", "  "))
		fmt.Fprintln(os.Stderr, "\nput [container] [object]")
		fmt.Fprintln(os.Stderr, brimtext.Wrap(`
Performs a PUT request. A PUT to an account or container will create them. A PUT to an object will create it using the content from standard input.
        `, 0, "  ", "  "))
		fmt.Fprintln(os.Stderr, "\n[container] [object] can also be specified as [container]/[object]")
	} else {
		msg := err.Error()
		if strings.HasPrefix(msg, "flag provided but not defined: ") {
			msg = "No such option: " + msg[len("flag provided but not defined: "):]
		}
		fmt.Fprintln(os.Stderr, msg)
	}
	os.Exit(1)
}

func helpFlags(flags *flag.FlagSet) {
	var data [][]string
	firstWidth := 0
	flags.VisitAll(func(f *flag.Flag) {
		n := "    -" + f.Name
		u := strings.TrimSpace(f.Usage)
		if u != "" && u[0] == '|' {
			s := strings.SplitN(u, "|", 3)
			if len(s) == 3 {
				n += " " + strings.TrimSpace(s[1])
				u = strings.TrimSpace(s[2])
			}
		}
		if len(n) > firstWidth {
			firstWidth = len(n)
		}
		data = append(data, []string{n, u})
	})
	opts := brimtext.NewDefaultAlignOptions()
	opts.Widths = []int{0, brimtext.GetTTYWidth() - firstWidth - 2}
	fmt.Fprint(os.Stderr, brimtext.Align(data, opts))
}

func main() {
	if err := globalFlags.Parse(os.Args[1:]); err != nil || len(globalFlags.Args()) == 0 {
		help(globalFlags, err)
	}
	if *globalFlagAuthURL == "" {
		help(globalFlags, errors.New("No Auth URL set; use -A"))
	}
	if *globalFlagAuthUser == "" {
		help(globalFlags, errors.New("No Auth User set; use -U"))
	}
	if *globalFlagAuthKey == "" && *globalFlagAuthPassword == "" {
		help(globalFlags, errors.New("No Auth Key or Password set; use -K or -P"))
	}
	c, resp := client.NewClient(*globalFlagAuthTenant, *globalFlagAuthUser, *globalFlagAuthPassword, *globalFlagAuthKey, *globalFlagStorageRegion, *globalFlagAuthURL, *globalFlagInternalStorage)
	if resp != nil {
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		fatal("Auth responded with %d %s - %s", resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
	}
	cmd := ""
	args := append([]string{}, globalFlags.Args()...)
	if len(args) > 0 {
		cmd = args[0]
		args = args[1:]
	}
	switch cmd {
	case "delete":
		delet(c, args)
	case "get":
		get(c, args)
	case "head":
		head(c, args)
	case "post":
		post(c, args)
	case "put":
		put(c, args)
	default:
		help(globalFlags, fmt.Errorf("Unknown command: %s", cmd))
	}
}

func delet(c client.Client, args []string) {
	container, object := parsePath(args)
	var resp *http.Response
	if object != "" {
		resp = c.DeleteObject(container, object, globalFlagHeaders.Headers())
	} else if container != "" {
		resp = c.DeleteContainer(container, globalFlagHeaders.Headers())
	} else {
		resp = c.DeleteAccount(globalFlagHeaders.Headers())
	}
	if resp.StatusCode/100 != 2 {
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		fatal("%d %s - %s", resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
	}
	resp.Body.Close()
}

func get(c client.Client, args []string) {
	if err := getFlags.Parse(args); err != nil {
		help(getFlags, err)
	}
	container, object := parsePath(getFlags.Args())
	if *getFlagRaw || object != "" {
		var resp *http.Response
		if object != "" {
			resp = c.GetObject(container, object, globalFlagHeaders.Headers())
		} else if container != "" {
			resp = c.GetContainerRaw(container, *getFlagMarker, *getFlagEndMarker, *getFlagLimit, *getFlagPrefix, *getFlagDelimiter, *getFlagReverse, globalFlagHeaders.Headers())
		} else {
			resp = c.GetAccountRaw(*getFlagMarker, *getFlagEndMarker, *getFlagLimit, *getFlagPrefix, *getFlagDelimiter, *getFlagReverse, globalFlagHeaders.Headers())
		}
		if resp.StatusCode/100 != 2 {
			bodyBytes, _ := ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			fatal("%d %s - %s", resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
		}
		if *getFlagRaw || object == "" {
			data := [][]string{}
			ks := []string{}
			for k := range resp.Header {
				ks = append(ks, k)
			}
			sort.Strings(ks)
			for _, k := range ks {
				for _, v := range resp.Header[k] {
					data = append(data, []string{k + ":", v})
				}
			}
			fmt.Println(resp.StatusCode, http.StatusText(resp.StatusCode))
			opts := brimtext.NewDefaultAlignOptions()
			fmt.Print(brimtext.Align(data, opts))
		}
		if _, err := io.Copy(os.Stdout, resp.Body); err != nil {
			fatal("%s", err)
		}
		return
	}
	if container != "" {
		entries, resp := c.GetContainer(container, *getFlagMarker, *getFlagEndMarker, *getFlagLimit, *getFlagPrefix, *getFlagDelimiter, *getFlagReverse, globalFlagHeaders.Headers())
		if resp.StatusCode/100 != 2 {
			bodyBytes, _ := ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			fatal("%d %s - %s", resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
		}
		if *getFlagNameOnly {
			for _, entry := range entries {
				if entry.Subdir != "" {
					fmt.Println(entry.Subdir)
				} else {
					fmt.Println(entry.Name)
				}
			}
		} else {
			var data [][]string
			data = [][]string{{"Name", "Bytes", "Content Type", "Last Modified", "Hash"}}
			for _, entry := range entries {
				if entry.Subdir != "" {
					data = append(data, []string{entry.Subdir, "", "", "", ""})
				} else {
					data = append(data, []string{entry.Name, fmt.Sprintf("%d", entry.Bytes), entry.ContentType, entry.LastModified, entry.Hash})
				}
			}
			fmt.Print(brimtext.Align(data, nil))
		}
		return
	}
	entries, resp := c.GetAccount(*getFlagMarker, *getFlagEndMarker, *getFlagLimit, *getFlagPrefix, *getFlagDelimiter, *getFlagReverse, globalFlagHeaders.Headers())
	if resp.StatusCode/100 != 2 {
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		fatal("%d %s - %s", resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
	}
	if *getFlagNameOnly {
		for _, entry := range entries {
			fmt.Println(entry.Name)
		}
	} else {
		var data [][]string
		data = [][]string{{"Name", "Count", "Bytes"}}
		for _, entry := range entries {
			data = append(data, []string{entry.Name, fmt.Sprintf("%d", entry.Count), fmt.Sprintf("%d", entry.Bytes)})
		}
		fmt.Print(brimtext.Align(data, nil))
	}
	return
}

func head(c client.Client, args []string) {
	if err := headFlags.Parse(args); err != nil {
		help(headFlags, err)
	}
	container, object := parsePath(headFlags.Args())
	var kstrip string
	var translateHeaders map[string]string
	var resp *http.Response
	if object != "" {
		kstrip = "X-Object-"
		resp = c.HeadObject(container, object, globalFlagHeaders.Headers())
		translateHeaders = map[string]string{
			"Content-Length": "Bytes Used",
			"Content-Type":   "Content Type",
			"Etag":           "ETag",
			"Last-Modified":  "Last Modified",
		}
	} else if container != "" {
		kstrip = "X-Container-"
		resp = c.HeadContainer(container, globalFlagHeaders.Headers())
	} else {
		kstrip = "X-Account-"
		resp = c.HeadAccount(globalFlagHeaders.Headers())
	}
	bodyBytes, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		fatal("%d %s - %s", resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
	}
	data := [][]string{}
	ks := []string{}
	kls := map[string]string{}
	for k := range resp.Header {
		if *headFlagRaw {
			ks = append(ks, k)
			kls[k] = k
		} else if strings.HasPrefix(k, kstrip) {
			nk := strings.Replace(k[len(kstrip):], "-", " ", -1)
			ks = append(ks, nk)
			kls[nk] = k
		} else if translateHeaders[k] != "" {
			ks = append(ks, translateHeaders[k])
			kls[translateHeaders[k]] = k
		}
	}
	sort.Strings(ks)
	for _, k := range ks {
		for _, v := range resp.Header[kls[k]] {
			if *headFlagRaw {
				data = append(data, []string{k + ":", v})
			} else {
				data = append(data, []string{v, k})
			}
		}
	}
	if *headFlagRaw {
		fmt.Println(resp.StatusCode, http.StatusText(resp.StatusCode))
	}
	opts := brimtext.NewDefaultAlignOptions()
	if !*headFlagRaw {
		opts.Alignments = []brimtext.Alignment{brimtext.Right, brimtext.Left}
	}
	fmt.Print(brimtext.Align(data, opts))
}

func put(c client.Client, args []string) {
	container, object := parsePath(args)
	var resp *http.Response
	if object != "" {
		resp = c.PutObject(container, object, globalFlagHeaders.Headers(), os.Stdin)
	} else if container != "" {
		resp = c.PutContainer(container, globalFlagHeaders.Headers())
	} else {
		resp = c.PutAccount(globalFlagHeaders.Headers())
	}
	if resp.StatusCode/100 != 2 {
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		fatal("%d %s - %s", resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
	}
	resp.Body.Close()
}

func post(c client.Client, args []string) {
	container, object := parsePath(args)
	var resp *http.Response
	if object != "" {
		resp = c.PostObject(container, object, globalFlagHeaders.Headers())
	} else if container != "" {
		resp = c.PostContainer(container, globalFlagHeaders.Headers())
	} else {
		resp = c.PostAccount(globalFlagHeaders.Headers())
	}
	if resp.StatusCode/100 != 2 {
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		fatal("%d %s - %s", resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
	}
	resp.Body.Close()
}

func fatal(frmt string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, frmt, args...)
	fmt.Fprintln(os.Stderr)
	os.Exit(1)
}

func parsePath(args []string) (string, string) {
	if len(args) == 0 {
		return "", ""
	}
	path := ""
	for _, arg := range args {
		if path == "" {
			path = arg
			continue
		}
		if strings.HasSuffix(path, "/") {
			path += arg
		} else {
			path += "/" + arg
		}
	}
	parts := strings.SplitN(path, "/", 2)
	if len(parts) == 1 {
		return parts[0], ""
	}
	return parts[0], parts[1]
}

type stringListFlag []string

func (slf *stringListFlag) Set(value string) error {
	*slf = append(*slf, value)
	return nil
}

func (slf *stringListFlag) String() string {
	return strings.Join(*slf, " ")
}

func (slf *stringListFlag) Headers() map[string]string {
	headers := map[string]string{}
	for _, parameter := range *slf {
		splitParameters := strings.SplitN(parameter, ":", 2)
		if len(splitParameters) == 2 {
			headers[strings.TrimSpace(splitParameters[0])] = strings.TrimSpace(splitParameters[1])
		} else {
			headers[strings.TrimSpace(splitParameters[0])] = ""
		}
	}
	return headers
}
