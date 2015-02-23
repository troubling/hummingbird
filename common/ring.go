package common

import (
	"compress/gzip"
	"crypto/md5"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
)

type Device struct {
	Id              uint    `json:"id"`
	Device          string  `json:"device"`
	Ip              string  `json:"ip"`
	Meta            string  `json:"meta"`
	Port            uint    `json:"port"`
	Region          uint    `json:"region"`
	ReplicationIp   string  `json:"replication_ip"`
	ReplicationPort uint    `json:"replication_port"`
	Weight          float64 `json:"weight"`
	Zone            uint    `json:"zone"`
}

type Ring struct {
	Devs                []Device `json:"devs"`
	ReplicaCount        uint     `json:"replica_count"`
	PartShift           uint     `json:"part_shift"`
	replica2part2dev_id [][]uint16
	prefix              string
	suffix              string
}

func (r Ring) GetNodes(partition uint64) []Device {
	var response []Device
	for i := uint(0); i < r.ReplicaCount; i++ {
		response = append(response, r.Devs[r.replica2part2dev_id[i][partition]])
	}
	return response
}

func (r Ring) GetPartition(account string, container string, object string) uint64 {
	hash := md5.New()
	hash.Write([]byte(r.prefix + "/" + account + "/"))
	if container != "" {
		hash.Write([]byte(container + "/"))
		if object != "" {
			hash.Write([]byte(object + "/"))
		}
	}
	// treat as big endian unsigned int
	hash.Write([]byte(r.suffix))
	digest := hash.Sum([]byte(""))
	val := uint64(digest[0])<<24 | uint64(digest[1])<<16 | uint64(digest[2])<<8 | uint64(digest[3])
	return val >> r.PartShift
}

func LoadRing(path string, prefix string, suffix string) (*Ring, error) {
	fp, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			errMsg := fmt.Sprintf("File at %s doesn't exists", path)
			return nil, errors.New(errMsg)
		} else {
			return nil, err
		}
	}
	gz, _ := gzip.NewReader(fp)
	magicBuf := make([]byte, 4)
	io.ReadFull(gz, magicBuf)
	if string(magicBuf) != "R1NG" {
		return nil, errors.New("Bad magic string")
	}
	var ringVersion uint16
	binary.Read(gz, binary.BigEndian, &ringVersion)
	if ringVersion != 1 {
		return nil, errors.New(fmt.Sprintf("Unknown ring version %d", ringVersion))
	}
	// TODO: assert ringVersion == 1
	var json_len uint32
	binary.Read(gz, binary.BigEndian, &json_len)
	jsonBuf := make([]byte, json_len)
	io.ReadFull(gz, jsonBuf)
	var ring Ring
	json.Unmarshal(jsonBuf, &ring)
	ring.prefix = prefix
	ring.suffix = suffix
	partitionCount := 1 << (32 - ring.PartShift)
	for i := uint(0); i < ring.ReplicaCount; i++ {
		part2dev := make([]uint16, partitionCount)
		binary.Read(gz, binary.LittleEndian, &part2dev)
		ring.replica2part2dev_id = append(ring.replica2part2dev_id, part2dev)
	}
	return &ring, nil
}
