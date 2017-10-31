//  Copyright (c) 2016 Rackspace

package ec

import (
	"io"

	"github.com/klauspost/reedsolomon"
)

func ecSplit(dataChunks, parityChunks int, fp io.Reader, chunkSize int, contentLength int64, writers []io.WriteCloser) error {
	enc, err := reedsolomon.New(dataChunks, parityChunks)
	if err != nil {
		return err
	}
	data := make([][]byte, dataChunks+parityChunks)
	databuf := make([]byte, (dataChunks+parityChunks)*chunkSize)
	for i := range data {
		data[i] = databuf[i*chunkSize : (i+1)*chunkSize]
	}
	totalRead := int64(0)
	for totalRead < contentLength {
		expectedRead := dataChunks * chunkSize
		if contentLength-totalRead < int64(expectedRead) {
			expectedRead = int(contentLength - totalRead)
		}
		read, err := io.ReadFull(fp, databuf[:expectedRead])
		if err != nil && err != io.EOF {
			return err
		}
		if read == 0 {
			return io.ErrUnexpectedEOF
		}
		totalRead += int64(read)
		for read%dataChunks != 0 { // pad data with 0s to a multiple of dataChunks
			databuf[read] = 0
			read++
		}
		thisChunkSize := read / dataChunks
		for i := range data { // assign data chunks
			data[i] = databuf[i*thisChunkSize : (i+1)*thisChunkSize]
		}
		if err := enc.Encode(data); err != nil {
			return err
		}
		for i := range data {
			if writers[i] != nil {
				_, err := writers[i].Write(data[i])
				if err != nil {
					writers[i] = nil
				}
			}
		}
	}
	return nil
}

func ecGlue(dataChunks, parityChunks int, bodies []io.Reader, chunkSize int, contentLength int64, dsts ...io.Writer) error {
	enc, err := reedsolomon.New(dataChunks, parityChunks)
	if err != nil {
		return err
	}
	data := make([][]byte, dataChunks+parityChunks)
	databuf := make([]byte, (dataChunks+parityChunks)*chunkSize)
	totalWritten := int64(0)
	for totalWritten < contentLength {
		expectedChunkSize := chunkSize
		if contentLength-totalWritten < int64(chunkSize*dataChunks) {
			expectedChunkSize = int((contentLength - totalWritten) / int64(dataChunks))
			if (contentLength-totalWritten)%int64(dataChunks) != 0 {
				expectedChunkSize++
			}
		}
		for i := range data {
			if bodies[i] != nil {
				data[i] = databuf[i*expectedChunkSize : (i+1)*expectedChunkSize]
			}
		}
		for i := range bodies {
			if bodies[i] != nil {
				if _, err := io.ReadFull(bodies[i], data[i]); err != nil {
					data[i] = nil
					bodies[i] = nil
				}
			}
		}
		if err := enc.Reconstruct(data); err != nil {
			return err
		}
		for i := 0; i < dataChunks; i++ {
			if contentLength-totalWritten < int64(len(data[i])) { // strip off any padding
				data[i] = data[i][:contentLength-totalWritten]
			}
			for j, d := range dsts {
				if d != nil {
					if _, err := d.Write(data[i]); err != nil {
						dsts[j] = nil
					}
				}
			}
			totalWritten += int64(len(data[i]))
		}
	}
	return nil
}
