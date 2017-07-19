package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/gholt/brimtext"
	"github.com/troubling/hummingbird/client"
)

var (
	globalFlags               = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	globalFlagAuthURL         = globalFlags.String("A", os.Getenv("AUTH_URL"), "|<url>| URL to auth system, example: http://127.0.0.1:8080/auth/v1.0 - Env: AUTH_URL")
	globalFlagAuthTenant      = globalFlags.String("T", os.Getenv("AUTH_TENANT"), "|<tenant>| Tenant name for auth system, example: test - Not all auth systems need this. Env: AUTH_TENANT")
	globalFlagAuthUser        = globalFlags.String("U", os.Getenv("AUTH_USER"), "|<user>| User name for auth system, example: tester - Some auth systems allow tenant:user format here, example: test:tester - Env: AUTH_USER")
	globalFlagAuthKey         = globalFlags.String("K", os.Getenv("AUTH_KEY"), "|<key>| Key for auth system, example: testing - Some auth systems use passwords instead, see -P - Env: AUTH_KEY")
	globalFlagAuthPassword    = globalFlags.String("P", os.Getenv("AUTH_PASSWORD"), "|<password>| Password for auth system, example: testing - Some auth system use keys instead, see -K - Env: AUTH_PASSWORD")
	globalFlagStorageRegion   = globalFlags.String("R", os.Getenv("STORAGE_REGION"), "|<region>| Storage region to use if set, otherwise uses the default. Env: STORAGE_REGION")
	globalFlagVerbose         = globalFlags.Bool("v", false, "Will activate verbose output.")
	globalFlagContinueOnError = globalFlags.Bool("continue-on-error", false, "When possible, continue with additional operations even if one or more fail.")
	globalFlagConcurrency     *int               // defined in init()
	globalFlagInternalStorage *bool              // defined in init()
	globalFlagHeaders         = stringListFlag{} // defined in init()
)

var (
	downloadFlags       = flag.NewFlagSet("download", flag.ContinueOnError)
	downloadFlagAccount = downloadFlags.Bool("a", false, "Indicates you truly wish to download the entire account; this is to prevent accidentally doing so when giving a single parameter to download.")
)

var (
	getFlags         = flag.NewFlagSet("get", flag.ContinueOnError)
	getFlagRaw       = getFlags.Bool("r", false, "Emit raw results")
	getFlagNameOnly  = getFlags.Bool("n", false, "In listings, emits the names only")
	getFlagMarker    = getFlags.String("marker", "", "|<text>| In listings, sets the start marker")
	getFlagEndMarker = getFlags.String("endmarker", "", "|<text>| In listings, sets the stop marker")
	getFlagReverse   = getFlags.Bool("reverse", false, "In listings, reverses the order")
	getFlagLimit     = getFlags.Int("limit", 0, "|<number>| In listings, limits the results")
	getFlagPrefix    = getFlags.String("prefix", "", "|<text>| In listings, returns only those matching the prefix")
	getFlagDelimiter = getFlags.String("delimiter", "", "|<text>| In listings, sets the delimiter and activates delimiter listings")
)

var (
	headFlags   = flag.NewFlagSet("head", flag.ContinueOnError)
	headFlagRaw = headFlags.Bool("r", false, "Emit raw headers")
)

func init() {
	i32, _ := strconv.ParseInt(os.Getenv("CONCURRENCY"), 10, 32)
	globalFlagConcurrency = globalFlags.Int("C", int(i32), "|<number>| The maximum number of concurrent operations to perform; default is 1. Env: CONCURRENCY")
	b, _ := strconv.ParseBool(os.Getenv("STORAGE_INTERNAL"))
	globalFlagInternalStorage = globalFlags.Bool("I", b, "Internal storage URL resolution, such as Rackspace ServiceNet. Env: STORAGE_INTERNAL")
	globalFlags.Var(&globalFlagHeaders, "H", "|<name>:[value]| Sets a header to be sent with the request. Useful mostly for PUTs and POSTs, allowing you to set metadata. This option can be specified multiple times for additional headers.")
	var flagbuf bytes.Buffer
	globalFlags.SetOutput(&flagbuf)
	downloadFlags.SetOutput(&flagbuf)
	getFlags.SetOutput(&flagbuf)
	headFlags.SetOutput(&flagbuf)
}

func fatal(err error) {
	if err == flag.ErrHelp || err == nil {
		fmt.Println(os.Args[0], `[options] <subcommand> ...`)
		fmt.Println(brimtext.Wrap(`
Tool for accessing a Hummingbird/Swift cluster. Some global options can also be set via environment variables. These will be noted at the end of the description with Env: NAME. The following global options are available:
        `, 0, "  ", "  "))
		helpFlags(globalFlags)
		fmt.Println()
		fmt.Println(brimtext.Wrap(`
The following subcommands are available:`, 0, "", ""))
		fmt.Println("\ndelete [container] [object]")
		fmt.Println(brimtext.Wrap(`
Performs a DELETE request. A DELETE, as probably expected, is used to remove the target.
        `, 0, "  ", "  "))
		fmt.Println("\ndownload [options] [container] [object] <destpath>")
		fmt.Println(brimtext.Wrap(`
Downloads an object or objects to a local file or files. The <destpath> indicates where you want the file or files to be created. If you don't give [container] [object] the entire account will be downloaded (requires -a for confirmation). If you just give [container] that entire container will be downloaded. Perhaps obviously, if you give [container] [object] just that object will be downloaded.
        `, 0, "  ", "  "))
		helpFlags(downloadFlags)
		fmt.Println("\nget [options] [container] [object]")
		fmt.Println(brimtext.Wrap(`
Performs a GET request. A GET on an account or container will output the listing of containers or objects, respectively. A GET on an object will output the content of the object to standard output.
        `, 0, "  ", "  "))
		helpFlags(getFlags)
		fmt.Println("\nhead [options] [container] [object]")
		fmt.Println(brimtext.Wrap(`
Performs a HEAD request, giving overall information about the account, container, or object.
        `, 0, "  ", "  "))
		helpFlags(headFlags)
		fmt.Println("\npost [container] [object]")
		fmt.Println(brimtext.Wrap(`
Performs a POST request. POSTs allow you to update the metadata for the target.
        `, 0, "  ", "  "))
		fmt.Println("\nput [container] [object]")
		fmt.Println(brimtext.Wrap(`
Performs a PUT request. A PUT to an account or container will create them. A PUT to an object will create it using the content from standard input.
        `, 0, "  ", "  "))
		fmt.Println("\nupload [options] <sourcepath> [container] [object]")
		fmt.Println(brimtext.Wrap(`
Uploads local files as objects. If you don't specify [container] the name of the current directory will be used. If you don't specify [object] the relative path name from the current directory will be used. If you do specify [object] while uploading a directory, [object] will be used as a prefix to the resulting object names. Note that when uploading a directory, only regular files will be uploaded.
        `, 0, "  ", "  "))
		fmt.Println("\n[container] [object] can also be specified as [container]/[object]")
	} else {
		msg := err.Error()
		if strings.HasPrefix(msg, "flag provided but not defined: ") {
			msg = "No such option: " + msg[len("flag provided but not defined: "):]
		}
		fmt.Fprintln(os.Stderr, msg)
	}
	os.Exit(1)
}

func fatalf(frmt string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, frmt, args...)
	os.Exit(1)
}

func verbosef(frmt string, args ...interface{}) {
	if *globalFlagVerbose {
		fmt.Fprintf(os.Stderr, frmt, args...)
	}
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
	fmt.Print(brimtext.Align(data, opts))
}

func main() {
	if err := globalFlags.Parse(os.Args[1:]); err != nil || len(globalFlags.Args()) == 0 {
		fatal(err)
	}
	if *globalFlagAuthURL == "" {
		fatalf("No Auth URL set; use -A\n")
	}
	if *globalFlagAuthUser == "" {
		fatalf("No Auth User set; use -U\n")
	}
	if *globalFlagAuthKey == "" && *globalFlagAuthPassword == "" {
		fatalf("No Auth Key or Password set; use -K or -P\n")
	}
	c, resp := client.NewClient(*globalFlagAuthTenant, *globalFlagAuthUser, *globalFlagAuthPassword, *globalFlagAuthKey, *globalFlagStorageRegion, *globalFlagAuthURL, *globalFlagInternalStorage)
	if resp != nil {
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		fatalf("Auth responded with %d %s - %s\n", resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes))
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
	case "download":
		download(c, args)
	case "get":
		get(c, args)
	case "head":
		head(c, args)
	case "post":
		post(c, args)
	case "put":
		put(c, args)
	case "upload":
		upload(c, args)
	default:
		fatalf("Unknown command: %s\n", cmd)
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
		fatalf("%d %s - %s - %s\n", resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes), resp.Header.Get("X-Source-Code"))
	}
	resp.Body.Close()
}

func get(c client.Client, args []string) {
	if err := getFlags.Parse(args); err != nil {
		fatal(err)
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
			fatalf("%d %s - %s - %s\n", resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes), resp.Header.Get("X-Source-Code"))
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
			fatal(err)
		}
		return
	}
	if container != "" {
		entries, resp := c.GetContainer(container, *getFlagMarker, *getFlagEndMarker, *getFlagLimit, *getFlagPrefix, *getFlagDelimiter, *getFlagReverse, globalFlagHeaders.Headers())
		if resp.StatusCode/100 != 2 {
			bodyBytes, _ := ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			fatalf("%d %s - %s - %s\n", resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes), resp.Header.Get("X-Source-Code"))
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
		fatalf("%d %s - %s - %s\n", resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes), resp.Header.Get("X-Source-Code"))
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
		fatal(err)
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
		fatalf("%d %s - %s - %s\n", resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes), resp.Header.Get("X-Source-Code"))
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
		fatalf("%d %s - %s - %s\n", resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes), resp.Header.Get("X-Source-Code"))
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
		fatalf("%d %s - %s - %s\n", resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes), resp.Header.Get("X-Source-Code"))
	}
	resp.Body.Close()
}

func upload(c client.Client, args []string) {
	if len(args) == 0 {
		fatalf("<sourcepath> is required for upload.\n")
	}
	sourcepath := args[0]
	container, object := parsePath(args[1:])
	if container == "" {
		abscwd, err := filepath.Abs(".")
		if err != nil {
			fatalf("Could not determine current working directory: %s\n", err)
		}
		container = filepath.Base(abscwd)
	}
	verbosef("Ensuring container %q exists.\n", container)
	resp := c.PutContainer(container, globalFlagHeaders.Headers())
	if resp.StatusCode/100 != 2 {
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		fatalf("PUT %s - %d %s - %s - %s\n", container, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes), resp.Header.Get("X-Source-Code"))
	}
	resp.Body.Close()
	concurrency := *globalFlagConcurrency
	if concurrency < 1 {
		concurrency = 1
	}
	uploadChan := make(chan string, concurrency-1)
	wg := sync.WaitGroup{}
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			for {
				path := <-uploadChan
				if path == "" {
					break
				}
				verbosef("Uploading %q to %q %q.\n", path, container, object+path)
				f, err := os.Open(path)
				if err != nil {
					if *globalFlagContinueOnError {
						fmt.Fprintf(os.Stderr, "Cannot open %s while attempting to upload to %s/%s: %s\n", path, container, object+path, err)
						continue
					} else {
						fatalf("Cannot open %s while attempting to upload to %s/%s: %s\n", path, container, object+path, err)
					}
				}
				resp := c.PutObject(container, object+path, globalFlagHeaders.Headers(), f)
				if resp.StatusCode/100 != 2 {
					bodyBytes, _ := ioutil.ReadAll(resp.Body)
					resp.Body.Close()
					f.Close()
					if *globalFlagContinueOnError {
						fmt.Fprintf(os.Stderr, "PUT %s/%s - %d %s - %s - %s\n", container, object+path, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes), resp.Header.Get("X-Source-Code"))
						continue
					} else {
						fatalf("PUT %s/%s - %d %s - %s - %s\n", container, object+path, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes), resp.Header.Get("X-Source-Code"))
					}
				}
				resp.Body.Close()
				f.Close()
			}
			wg.Done()
		}()
	}
	fi, err := os.Stat(sourcepath)
	if err != nil {
		fatalf("Could not stat %s: %s\n", sourcepath, err)
	}
	// This "if" is so a single file upload that happens to be a symlink will work.
	if fi.Mode().IsRegular() {
		uploadChan <- sourcepath
	} else {
		// This "if" is to handle when the user-given path is a symlink to a directory; we normally want to skip symlinks, but not in this initial case.
		if !strings.HasSuffix(sourcepath, string(os.PathSeparator)) {
			sourcepath += string(os.PathSeparator)
		}
		filepath.Walk(sourcepath, func(path string, info os.FileInfo, err error) error {
			if err != nil || !info.Mode().IsRegular() {
				return nil
			}
			uploadChan <- path
			return nil
		})
	}
	close(uploadChan)
	wg.Wait()
}

func download(c client.Client, args []string) {
	if err := downloadFlags.Parse(args); err != nil {
		fatal(err)
	}
	args = downloadFlags.Args()
	if len(args) == 0 {
		fatalf("<destpath> is required for download.\n")
	}
	destpath := args[len(args)-1]
	container, object := parsePath(args[:len(args)-1])
	concurrency := *globalFlagConcurrency
	// Need at least 2 to queue object downloads while reading a container listing.
	if concurrency < 2 {
		concurrency = 2
	}
	type downloadTask struct {
		container string
		object    string
		destpath  string
	}
	downloadChan := make(chan *downloadTask, concurrency-1)
	var dirExistsLock sync.Mutex
	dirExists := map[string]bool{}
	taskWG := sync.WaitGroup{}
	taskWG.Add(concurrency)
	containerWG := sync.WaitGroup{}
	for i := 0; i < concurrency; i++ {
		go func() {
			for {
				task := <-downloadChan
				if task == nil {
					break
				}
				if task.object == "" {
					entries, resp := c.GetContainer(task.container, "", "", 0, "", "", false, globalFlagHeaders.Headers())
					if resp.StatusCode/100 != 2 {
						bodyBytes, _ := ioutil.ReadAll(resp.Body)
						resp.Body.Close()
						containerWG.Done()
						if *globalFlagContinueOnError {
							fmt.Fprintf(os.Stderr, "GET %s - %d %s - %s - %s\n", task.container, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes), resp.Header.Get("X-Source-Code"))
							continue
						} else {
							fatalf("GET %s - %d %s - %s - %s\n", task.container, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes), resp.Header.Get("X-Source-Code"))
						}
					}
					resp.Body.Close()
					for _, entry := range entries {
						if entry.Name != "" {
							downloadChan <- &downloadTask{container: task.container, object: entry.Name, destpath: filepath.Join(task.destpath, filepath.FromSlash(entry.Name))}
						}
					}
					containerWG.Done()
					continue
				}
				verbosef("Downloading %s/%s to %s.\n", task.container, task.object, task.destpath)
				if dstdr := filepath.Dir(task.destpath); dstdr != "." {
					dirExistsLock.Lock()
					if !dirExists[dstdr] {
						if err := os.MkdirAll(dstdr, 0755); err != nil {
							if *globalFlagContinueOnError {
								fmt.Fprintf(os.Stderr, "Could not make directory path %s: %s\n", dstdr, err)
							} else {
								fatalf("Could not make directory path %s: %s\n", dstdr, err)
							}
						}
						dirExists[dstdr] = true
					}
					dirExistsLock.Unlock()
				}
				f, err := os.Create(task.destpath)
				if err != nil {
					if *globalFlagContinueOnError {
						fmt.Fprintf(os.Stderr, "Could not create %s: %s\n", task.destpath, err)
						continue
					} else {
						fatalf("Could not create %s: %s\n", task.destpath, err)
					}
				}
				resp := c.GetObject(task.container, task.object, globalFlagHeaders.Headers())
				if resp.StatusCode/100 != 2 {
					bodyBytes, _ := ioutil.ReadAll(resp.Body)
					resp.Body.Close()
					f.Close()
					if *globalFlagContinueOnError {
						fmt.Fprintf(os.Stderr, "GET %s/%s - %d %s - %s - %s\n", task.container, task.object, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes), resp.Header.Get("X-Source-Code"))
						continue
					} else {
						fatalf("GET %s/%s - %d %s - %s - %s\n", task.container, task.object, resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes), resp.Header.Get("X-Source-Code"))
					}
				}
				if _, err = io.Copy(f, resp.Body); err != nil {
					resp.Body.Close()
					f.Close()
					if *globalFlagContinueOnError {
						fmt.Fprintf(os.Stderr, "Could not complete content transfer from %s/%s to %s: %s\n", task.container, task.object, task.destpath, err)
						continue
					} else {
						fatalf("Could not complete content transfer from %s/%s to %s: %s\n", task.container, task.object, task.destpath, err)
					}
				}
				resp.Body.Close()
				f.Close()
			}
			taskWG.Done()
		}()
	}
	if object != "" {
		fi, err := os.Stat(destpath)
		if err != nil {
			if !os.IsNotExist(err) {
				fatalf("Could not stat %s: %s\n", destpath, err)
			}
		} else if fi.IsDir() {
			destpath = filepath.Join(destpath, object)
		}
		downloadChan <- &downloadTask{container: container, object: object, destpath: destpath}
	} else if container != "" {
		fi, err := os.Stat(destpath)
		if err != nil {
			if !os.IsNotExist(err) {
				fatalf("Could not stat %s: %s\n", destpath, err)
			}
		} else if !fi.IsDir() {
			fatalf("Cannot download a container to a single file: %s\n", destpath)
		}
		containerWG.Add(1)
		downloadChan <- &downloadTask{container: container, object: "", destpath: destpath}
	} else if !*downloadFlagAccount {
		fatalf("You must specify -a if you wish to download the entire account.\n")
	} else {
		fi, err := os.Stat(destpath)
		if err != nil {
			if !os.IsNotExist(err) {
				fatalf("Could not stat %s: %s\n", destpath, err)
			}
		} else if !fi.IsDir() {
			fatalf("Cannot download an account to a single file: %s\n", destpath)
		}
		entries, resp := c.GetAccount("", "", 0, "", "", false, globalFlagHeaders.Headers())
		if resp.StatusCode/100 != 2 {
			bodyBytes, _ := ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			fatalf("GET - %d %s - %s - %s\n", resp.StatusCode, http.StatusText(resp.StatusCode), string(bodyBytes), resp.Header.Get("X-Source-Code"))
		}
		resp.Body.Close()
		for _, entry := range entries {
			if entry.Name != "" {
				containerWG.Add(1)
				downloadChan <- &downloadTask{container: entry.Name, object: "", destpath: filepath.Join(destpath, entry.Name)}
			}
		}
	}
	containerWG.Wait()
	close(downloadChan)
	taskWG.Wait()
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
