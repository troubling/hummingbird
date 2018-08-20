package middleware

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"encoding/json"
	"net/http"

	"github.com/troubling/hummingbird/common/conf"
	"github.com/troubling/hummingbird/common/srv"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

type decryptionWriter struct {
	log    srv.LowLevelLogger
	key    []byte
	writer http.ResponseWriter
	header http.Header
	metric tally.Counter
	stream cipher.Stream
}

func (dw *decryptionWriter) Header() http.Header {
	// We keep our own headers because the underlying dw.writer.Header() will
	// likely filter out a lot of the headers we care about.
	return dw.header
}

func (dw *decryptionWriter) Write(p []byte) (int, error) {
	if dw.stream != nil {
		dw.stream.XORKeyStream(p, p)
	}
	return dw.writer.Write(p)
}

func (dw *decryptionWriter) WriteHeader(statusCode int) {
	writerHeader := dw.writer.Header()
	for k := range dw.header {
		writerHeader.Set(k, dw.header.Get(k))
	}
	dw.writer.WriteHeader(statusCode)
	jsonValue := dw.header.Get("X-Object-Sysmeta-Crypto-Body-Meta")
	if jsonValue == "" {
		return
	}
	structValue := &struct {
		IV      string `json:"iv"`
		Cipher  string `json:"cipher"`
		BodyKey struct {
			Key string `json:"key"`
			IV  string `json:"iv"`
		} `json:"body_key"`
	}{}
	if err := json.Unmarshal([]byte(jsonValue), structValue); err != nil {
		dw.log.Error("invalid X-Object-Sysmeta-Crypto-Body-Meta", zap.String("value", jsonValue), zap.Error(err))
		return
	}
	if structValue.Cipher != "AES_CTR_256" {
		dw.log.Error("unknown cipher", zap.String("cipher", structValue.Cipher))
		return
	}
	block, err := aes.NewCipher(dw.key)
	if err != nil {
		dw.log.Error("aes.NewCipher", zap.Error(err))
		return
	}
	key, err := base64.StdEncoding.DecodeString(structValue.BodyKey.Key)
	if err != nil {
		dw.log.Error("bad structValue.BodyKey.Key", zap.Error(err))
		return
	}
	iv, err := base64.StdEncoding.DecodeString(structValue.BodyKey.IV)
	if err != nil {
		dw.log.Error("bad structValue.BodyKey.IV", zap.Error(err))
		return
	}
	cipher.NewCTR(block, iv).XORKeyStream(key, key)
	block, err = aes.NewCipher(key)
	if err != nil {
		dw.log.Error("aes.NewCipher", zap.Error(err))
		return
	}
	iv, err = base64.StdEncoding.DecodeString(structValue.IV)
	if err != nil {
		dw.log.Error("bad structValue.IV", zap.Error(err))
		return
	}
	dw.stream = cipher.NewCTR(block, iv)
	dw.metric.Inc(1)
	// TODO: GLH: Does not handle ranged GETs.
	//            My initial thoughts on how to implement ranged GETs is to
	//            compute the aes block aligned offset, increment iv
	//            appropriately, and save the aligned offset remainder to be
	//            skipped when dw.Write is called. See the osscrypt/main.go
	//            DecryptPartial for a bit more specifics.
}

func NewEncryption(config conf.Section, metricsScope tally.Scope) (func(http.Handler) http.Handler, error) {
	RegisterInfo("encryption", map[string]interface{}{})
	encryptedGetRequests := metricsScope.Counter("encrypted_get_requests")
	encryptedPutRequests := metricsScope.Counter("encrypted_put_requests")
	enabled := !config.GetBool("disable_encryption", false)
	var key []byte
	if enabled {
		// TODO: GLH: Waiting on PR for OSSCryptSecret; for now, fake the key.
		key = []byte{
			0, 1, 2, 3, 4, 5, 6, 7,
			0, 1, 2, 3, 4, 5, 6, 7,
			0, 1, 2, 3, 4, 5, 6, 7,
			0, 1, 2, 3, 4, 5, 6, 7,
		}
		// key, _, err := conf.OSSCryptSecret()
		// if err != nil {
		//     enabled = false
		//     // No idea how to log here, but we probably should.
		//     fmt.Println("WARNING: Could not load key for [filter:encryption] use; disabling filter")
		// }
	}
	if len(key) == 0 {
		enabled = false
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			if !enabled {
				next.ServeHTTP(writer, request)
				return
			}
			_, _, _, object := getPathParts(request)
			if object == "" {
				next.ServeHTTP(writer, request)
				return
			}
			switch request.Method {
			case "GET":
				next.ServeHTTP(&decryptionWriter{log: GetProxyContext(request).log, key: key, writer: writer, header: http.Header{}, metric: encryptedGetRequests}, request)
			case "PUT":
				// TODO: GLH
				encryptedPutRequests.Inc(1)
				next.ServeHTTP(writer, request)
			default:
				next.ServeHTTP(writer, request)
			}
		})
	}, nil
}
