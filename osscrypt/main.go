// This is just a proof of concept to get the general encryption routines working.

package main

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Printf(`
%s encrypt <key>
    Encrypts stdin to stdout using the key given.

%s decrypt <key>
    Decrypts stdin to stdout using the key given.

%s decrypt-partial <key> <offset> <length>
    Decrypts stdin to stdout using the key given, but only from the offset
    given for the length of bytes given.

<key> is a base64 encoded 32 byte value,
such as: "MDAmOzssnpa70wgyFxokXug5Yxo3iSylPlBC9Wn87lU="
`, os.Args[0], os.Args[0], os.Args[0])
		os.Exit(1)
	}
	subcommand := os.Args[1]
	key, err := base64.StdEncoding.DecodeString(os.Args[2])
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	switch subcommand {
	case "encrypt":
		if err := Encrypt(os.Stdin, os.Stdout, key); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	case "decrypt":
		if err := Decrypt(os.Stdin, os.Stdout, key); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	case "decrypt-partial":
		if len(os.Args) < 5 {
			fmt.Printf(`
%s decrypt-partial <key> <offset> <length>
    Decrypts stdin to stdout using the key given, but only from the offset
    given for the length of bytes given.
`, os.Args[0])
			os.Exit(1)
		}
		offset, err := strconv.Atoi(os.Args[3])
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		length, err := strconv.Atoi(os.Args[4])
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		if err := DecryptPartial(os.Stdin, os.Stdout, key, offset, length); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	default:
		fmt.Printf("unknown subcommand %q", subcommand)
		os.Exit(1)
	}
}

// Encrypt shows how to encrypt object content, reading plaintext version and
// writing encrypted version. We don't support ranged PUTs so the whole of the
// object content is always PUT, which simplifies things. Note that this writes
// the iv into the file, so the output will be 16 bytes larger than the input.
// Once we add this to Hummingbird for real, the iv will be stored in the
// object metadata.
func Encrypt(r io.Reader, w io.Writer, key []byte) error {
	if len(key) != 32 {
		return fmt.Errorf("key length %d != 32", len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return err
	}
	iv := make([]byte, aes.BlockSize)
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return err
	}
	// Normally this iv value would be saved in the object metadata. For now,
	// we're just storing at the beginning of the output.
	if _, err := w.Write(iv); err != nil {
		return err
	}
	_, err = io.Copy(&cipher.StreamWriter{S: cipher.NewCTR(block, iv), W: w}, r)
	return err
}

// Decrypt shows how to decrypt object content, like with a full GET. Note this
// assumes the iv is written at the beginning of the reader, and that the
// reader therefore has 16 more bytes than the actual content. Once we add this
// to Hummingbird for real, the iv will be stored in the object metadata.
func Decrypt(r io.Reader, w io.Writer, key []byte) error {
	if len(key) != 32 {
		return fmt.Errorf("key length %d != 32", len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return err
	}
	iv := make([]byte, aes.BlockSize)
	// Normally this iv value would be saved in the object metadata. For now,
	// we're assuming it was stored at the beginning of the input.
	if _, err := io.ReadFull(r, iv); err != nil {
		return err
	}
	_, err = io.Copy(w, &cipher.StreamReader{S: cipher.NewCTR(block, iv), R: r})
	return err
}

// DecryptPartial shows how to decrypt partial object content, like with a
// ranged GET. Note this assumes the iv is written at the beginning of the
// reader, and that the reader therefore has 16 more bytes than the actual
// content. Once we add this to Hummingbird for real, the iv will be stored in
// the object metadata, and the ranged GET to the backend object server will be
// block-aligned with the proxy discarding any `offset % 16`.
func DecryptPartial(r io.Reader, w io.Writer, key []byte, offset, length int) error {
	if len(key) != 32 {
		return fmt.Errorf("key length %d != 32", len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return err
	}
	iv := make([]byte, aes.BlockSize)
	// Normally this iv value would be saved in the object metadata. For now,
	// we're assuming it was stored at the beginning of the input.
	if _, err := io.ReadFull(r, iv); err != nil {
		return err
	}
	offsetBlock := offset / aes.BlockSize
	offsetRemainder := offset % aes.BlockSize
	// Seek to the offset, block-aligned. This kind of simulates a ranged GET
	// in that the first block-aligned bytes would not be returned to the proxy
	// server at all.
	if _, err := io.CopyN(ioutil.Discard, r, int64(offsetBlock*aes.BlockSize)); err != nil {
		return err
	}
	IncIV(iv, offset/aes.BlockSize)
	streamReader := &cipher.StreamReader{S: cipher.NewCTR(block, iv), R: r}
	// Seek past the few bytes we don't really want but got because of
	// block-alignment.
	if _, err = io.CopyN(ioutil.Discard, streamReader, int64(offsetRemainder)); err != nil {
		return err
	}
	_, err = io.CopyN(w, streamReader, int64(length))
	return err
}

// IncIV increments the iv by the v value, as if v blocks had been read.
func IncIV(iv []byte, v int) {
	// There has got to be a faster way to do this, but for now...
	for ; v > 0; v-- {
		for i := len(iv) - 1; i >= 0; i-- {
			iv[i]++
			if iv[i] != 0 {
				break
			}
		}
	}
}
