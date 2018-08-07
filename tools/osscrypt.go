package tools

import (
	"encoding/base64"
	"flag"
	"fmt"
	"os"

	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/srv"
)

// OSSCrypt implements the `hummingbird osscrypt` CLI tool and displays
// information about the OpenStack Swift Style Encryption layer's
// configuration.
func OSSCrypt(flags *flag.FlagSet, cnf srv.ConfigLoader) bool {
	secret, source, err := conf.OSSCryptSecret()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return false
	}
	if secret == nil {
		fmt.Fprintln(os.Stderr, "no osscrypt configuration found")
		return false
	}
	fmt.Printf("Using stored secret %q set in %s.\n", base64.StdEncoding.EncodeToString(secret), source)
	return true
}
