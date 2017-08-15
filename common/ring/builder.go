//  Copyright (c) 2017 Rackspace
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
//  implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package ring

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/troubling/hummingbird/common/pickle"
)

const (
	NONE_DEV                 uint    = 65535
	MAX_BALANCE              float64 = 999.99
	MAX_BALANCE_GATHER_COUNT int     = 3
)

func maxInt(x, y int) int {
	if x > y {
		return x
	} else {
		return y
	}
}

// devIterator returns a function to iterate over each device, and the first device.  It will return nil when no other devices are available.
func devIterator(devs []*RingBuilderDevice) (func() *RingBuilderDevice, *RingBuilderDevice) {
	var index int = 0
	var numDevs = len(devs)
	iter := func() *RingBuilderDevice {
		for index < numDevs && devs[index] == nil {
			index++
		}
		if index >= numDevs {
			return nil
		}
		devIndex := index
		index++
		return devs[devIndex]
	}
	return iter, iter()
}

type lastPartMovesArray struct {
	ArrayType string // should be "B"
	Data      []byte
}

type replica2Part2DevArray struct {
	ArrayType string // should be "H"
	Data      []uint
}

// RingBuilderPickle is used for pickling/unpickling ringbuilder data
type RingBuilderPickle struct {
	LastPartGatherStart int64                   `pickle:"_last_part_gather_start"`
	LastPartMovesEpoch  int64                   `pickle:"_last_part_moves_epoch"`
	PartPower           int64                   `pickle:"part_power"`
	DevsChanged         bool                    `pickle:"devs_changed"`
	Replicas            float64                 `pickle:"replicas"`
	MinPartHours        int64                   `pickle:"min_part_hours"`
	Parts               int64                   `pickle:"parts"`
	Overload            float64                 `pickle:"overload"`
	Dispersion          float64                 `pickle:"dispersion"`
	Version             int64                   `pickle:"version"`
	Devs                []*RingBuilderDevice    `pickle:"devs"`
	RemoveDevs          []*RingBuilderDevice    `pickle:"_remove_devs"`
	LastPartMoves       lastPartMovesArray      `pickle:"_last_part_moves"`
	Replica2Part2Dev    []replica2Part2DevArray `pickle:"_replica2part2dev"`
	//DispersionGraph     map[pickle.PickleTuple][]interface{} `pickle:"_dispersion_graph"`
}

type RingBuilderDevice struct {
	ReplicationPort int64   `pickle:"replication_port"`
	Meta            string  `pickle:"meta"`
	PartsWanted     int64   `pickle:"parts_wanted"`
	Device          string  `pickle:"device"`
	Zone            int64   `pickle:"zone"`
	Weight          float64 `pickle:"weight"`
	Ip              string  `pickle:"ip"`
	Region          int64   `pickle:"region"`
	Port            int64   `pickle:"port"`
	ReplicationIp   string  `pickle:"replication_ip"`
	Parts           int64   `pickle:"parts"`
	Id              int64   `pickle:"id"`
	tiers           [4]string
}

type RingBuilder struct {
	PartPower           int
	Replicas            float64
	MinPartHours        int
	Parts               int
	Overload            float64
	Devs                []*RingBuilderDevice
	DevsChanged         bool
	Version             int
	Dispersion          float64
	lastPartMoves       []byte
	lastPartMovesEpoch  int64
	lastPartGatherStart int
	partMovedBitmap     []byte
	replica2Part2Dev    [][]uint
	Debug               bool
	removedDevs         []*RingBuilderDevice
}

type minMax struct {
	min float64
	max float64
}

type replicaPlan struct {
	min    float64
	target float64
	max    float64
}

type devReplica struct {
	dev     *RingBuilderDevice
	replica int
}

type partReplicas struct {
	part     uint
	replicas []uint
}

func NewRingBuilder(partPower int, replicas float64, minPartHours int, debug bool) (*RingBuilder, error) {
	if partPower > 32 {
		return nil, fmt.Errorf("Part Power must be at most 32 (was %d)", partPower)
	}
	if replicas < 1 {
		return nil, fmt.Errorf("Replicas must be at least 1 (was %f)", replicas)
	}
	if minPartHours < 0 {
		return nil, fmt.Errorf("Min Part Hours must be non-negative (was %d)", minPartHours)
	}
	builder := &RingBuilder{
		PartPower:          partPower,
		Replicas:           replicas,
		MinPartHours:       minPartHours,
		Parts:              int(math.Exp2(float64(partPower))),
		DevsChanged:        false,
		Version:            0,
		lastPartMovesEpoch: 0,
		Dispersion:         0.0,
		Overload:           0.0,
		Debug:              debug,
	}

	// NOTE: In the python version, these were lazy created.
	builder.lastPartMoves = make([]byte, builder.Parts)
	builder.partMovedBitmap = make([]byte, maxInt(int(math.Exp2(float64(partPower-3))), 1))
	// TODO: Not sure if something needs to be done here to really support fractional replicas
	builder.replica2Part2Dev = make([][]uint, int(replicas))
	for i := 0; i < int(replicas); i++ {
		builder.replica2Part2Dev[i] = make([]uint, 0, builder.Parts)
	}

	return builder, nil
}

func NewRingBuilderFromFile(builderPath string, debug bool) (*RingBuilder, error) {

	f, err := os.Open(builderPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	buff := make([]byte, fi.Size())
	_, err = f.Read(buff)
	rbp := RingBuilderPickle{}
	err = pickle.Unmarshal(buff, &rbp)
	if err != nil {
		return nil, err
	}

	builder := &RingBuilder{
		PartPower:           int(rbp.PartPower),
		Replicas:            rbp.Replicas,
		MinPartHours:        int(rbp.MinPartHours),
		Parts:               int(rbp.Parts),
		Overload:            rbp.Overload,
		DevsChanged:         rbp.DevsChanged,
		Version:             int(rbp.Version),
		Dispersion:          rbp.Dispersion,
		lastPartMovesEpoch:  rbp.LastPartMovesEpoch,
		lastPartGatherStart: int(rbp.LastPartGatherStart),
		Debug:               debug,
	}
	builder.lastPartGatherStart = 0
	builder.lastPartMoves = rbp.LastPartMoves.Data
	builder.replica2Part2Dev = make([][]uint, len(rbp.Replica2Part2Dev))
	for i := 0; i < len(rbp.Replica2Part2Dev); i++ {
		builder.replica2Part2Dev[i] = rbp.Replica2Part2Dev[i].Data
	}
	builder.partMovedBitmap = make([]byte, maxInt(int(math.Exp2(float64(builder.PartPower-3))), 1))
	builder.Devs = make([]*RingBuilderDevice, len(rbp.Devs))
	copy(builder.Devs, rbp.Devs)
	builder.removedDevs = make([]*RingBuilderDevice, len(rbp.RemoveDevs))
	copy(builder.removedDevs, rbp.RemoveDevs)

	return builder, nil
}

// debug prints a debug message if debug is enabled
func (b *RingBuilder) debug(msg string) {
	if b.Debug {
		fmt.Println(msg)
	}
}

// Save serializes this RingBuilder instance to disk
func (b *RingBuilder) Save(builderPath string) error {
	f, err := os.OpenFile(builderPath, os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		return err
	}
	defer f.Close()
	rbp := RingBuilderPickle{
		PartPower:           int64(b.PartPower),
		Replicas:            b.Replicas,
		MinPartHours:        int64(b.MinPartHours),
		Parts:               int64(b.Parts),
		Overload:            b.Overload,
		DevsChanged:         b.DevsChanged,
		Version:             int64(b.Version),
		Dispersion:          b.Dispersion,
		LastPartMovesEpoch:  int64(b.lastPartMovesEpoch),
		LastPartGatherStart: int64(b.lastPartGatherStart),
	}

	rbp.LastPartMoves = lastPartMovesArray{
		ArrayType: "B",
		Data:      b.lastPartMoves,
	}
	rbp.Replica2Part2Dev = make([]replica2Part2DevArray, len(b.replica2Part2Dev))
	for i := 0; i < len(b.replica2Part2Dev); i++ {
		rbp.Replica2Part2Dev[i] = replica2Part2DevArray{
			ArrayType: "H",
			Data:      b.replica2Part2Dev[i],
		}
	}
	rbp.Devs = b.Devs
	rbp.RemoveDevs = b.removedDevs
	f.Write(pickle.PickleDumps(rbp))
	return nil
}

func (b *RingBuilder) setPartMoved(part uint) {
	b.lastPartMoves[part] = 0
	b.partMovedBitmap[part/8] |= (128 >> (part % 8))
}

func (b *RingBuilder) hasPartMoved(part uint) bool {
	//return bool(b.partMovedBitmap[part/8] & (128 >> (part % 8)))
	//NOTE: Not sure if this is the right thing yet
	return b.partMovedBitmap[part/8]&(128>>(part%8)) > 0
}

func (b *RingBuilder) canPartMove(part uint) bool {
	return int(b.lastPartMoves[part]) >= b.MinPartHours && !b.hasPartMoved(part)
}

// MinPartSecondsLeft returns the total seconds until a reblanace can be performed.
func (b *RingBuilder) MinPartSecondsLeft() int {
	elapsed := int(time.Now().Unix() - b.lastPartMovesEpoch)
	return maxInt(b.MinPartHours*3600-elapsed, 0)
}

// WeightOfOnePart returns the weight of each partition as calculated from the total weight of all the devices.
func (b *RingBuilder) WeightOfOnePart() float64 {
	totalWeight := 0.0
	for next, dev := devIterator(b.Devs); dev != nil; dev = next() {
		totalWeight += dev.Weight
	}
	return float64(b.Parts) * b.Replicas / totalWeight
}

// Set the weight of a device.  This should be called rather than just altering the weight directly, as the builder will need to rebuild some internal state to reflect the change.
func (b *RingBuilder) SetDevWeight(devId int64, weight float64) error {
	for _, dev := range b.removedDevs {
		if devId == dev.Id {
			return errors.New(fmt.Sprintf("Can not set weight of devId %d because it is marked for removal", devId))
		}
	}
	b.Devs[devId].Weight = weight
	b.DevsChanged = true
	b.Version += 1

	return nil
}

// Remove a device from the ring.
func (b *RingBuilder) RemoveDev(devId int64) {
	b.Devs[devId].Weight = 0
	b.removedDevs = append(b.removedDevs, b.Devs[devId])
	b.DevsChanged = true
	b.Version += 1
}

// updateLatPartMoves updates how many hours ago each partition was moved based on the current time.  The builder won't move a partition that has been moved more recently than minPartHours.
func (b *RingBuilder) updateLastPartMoves() {
	elapsedHours := uint8((time.Now().Unix() - b.lastPartMovesEpoch) / 3600)
	if elapsedHours <= 0 {
		return
	}
	for part := 0; part < b.Parts; part++ {
		lastPlusElapsed := b.lastPartMoves[part] + elapsedHours
		if lastPlusElapsed < 0xff {
			b.lastPartMoves[part] = lastPlusElapsed
		} else {
			b.lastPartMoves[part] = 0xff
		}
	}
	b.lastPartMovesEpoch = time.Now().Unix()
}

// buildWeightedReplicasByTier returns a map <tier> => replicanths for all tiers in the ring based on their weights.
func (b *RingBuilder) buildWeightedReplicasByTier() (map[string]float64, error) {
	weightOfOnePart := b.WeightOfOnePart()

	devicesWithRoom := make([]int64, 0, len(b.Devs))
	weightedReplicasForDev := make(map[int64]float64)
	for next, dev := devIterator(b.Devs); dev != nil; dev = next() {
		if dev.Weight <= 0.0 {
			continue
		}
		weightedReplicas := dev.Weight * weightOfOnePart / float64(b.Parts)
		if weightedReplicas < 1.0 {
			devicesWithRoom = append(devicesWithRoom, dev.Id)
		} else {
			weightedReplicas = 1.0
		}
		weightedReplicasForDev[dev.Id] = weightedReplicas
	}

	for true {
		remaining := b.Replicas
		for _, weight := range weightedReplicasForDev {
			remaining -= weight
		}
		if remaining < 1e-10 {
			break
		}
		d := devicesWithRoom[:0]
		relWeight := 0.0
		// TODO: we might be able to combine the following loops
		for _, id := range devicesWithRoom {
			if weightedReplicasForDev[id] < 1.0 {
				d = append(d, id)
				relWeight += weightedReplicasForDev[id]
			}
		}
		devicesWithRoom = d
		relWeight = remaining / relWeight
		for _, id := range devicesWithRoom {
			weightedReplicasForDev[id] = math.Min(1.0, weightedReplicasForDev[id]*(relWeight+1.0))
		}
	}

	weightedReplicasByTier := make(map[string]float64)
	for next, dev := devIterator(b.Devs); dev != nil; dev = next() {
		if dev.Weight <= 0.0 {
			continue
		}
		assignedReplicanths := weightedReplicasForDev[dev.Id]
		devTier := b.tiersForDev(dev)
		for _, tier := range devTier {
			weightedReplicasByTier[tier] += assignedReplicanths
		}

	}

	// Test that the for every level of the tier that the sum of weightedReplicas is very close to the total number of replicas for the ring
	replicasAtTier := [4]float64{0.0, 0.0, 0.0, 0.0}
	for t, w := range weightedReplicasByTier {
		if t != "" {
			replicasAtTier[strings.Count(t, ";")] += w
		}
	}
	for t, r := range replicasAtTier {
		if math.Abs(b.Replicas-r) > 1e-10 {
			return nil, errors.New(fmt.Sprintf("%f != %f at tier %d", r, b.Replicas, t))
		}
	}

	return weightedReplicasByTier, nil
}

// buildTier2Children wraps buildTierTree to exclude zero-weight devices.
func (b *RingBuilder) buildTier2Children() map[string][]string {
	weightedDevs := make([]*RingBuilderDevice, 0, len(b.Devs))
	for i := range b.Devs {
		if b.Devs[i] != nil && b.Devs[i].Weight > 0.0 {
			weightedDevs = append(weightedDevs, b.Devs[i])
		}
	}
	return b.buildTierTree(weightedDevs)
}

// buildMaxReplicasByTier returns a map of tier -> replicaCount for all teirs in the ring excluding zero weight devices.
//
// There will always be a "" entry as the root of the structure, whose replicaCount will equal the ring's replicaCount.
//
// There will be "region" entries for each region, including the maximum number of replicas the region might have for any given partition.
//
// Next there will be "region;zone" entries for each zone, indicating the maximum number of replicas in a given region and zone.  Anything greater than 1 indicates a partition at slightly elevated risk, as if that zone were to fail mupltiple replicas of that partition would be unreachable.
//
// Next there will be "region;zone;ip:port" entries for each node, indicating the maximum number of replicas stored on a node in a give region and zone.  Anything greater than 1 indicates a partition at elevated risk, as if that ip:port were to fail, multiple replicas of that partition would be unreachable.
//
// Last there will be "region;zone;ip:port;device" entries for each device, indicating the maximum number of replicas the device shares with other devices on the same node for any given partition.  Anything greater than 1 indicates a partition at serious risk, as the data on that partition will not be store distictly at the ring's replicaCount.
//
// Example returned map:
//
func (b *RingBuilder) buildMaxReplicasByTier() map[string]float64 {
	tier2Children := b.buildTier2Children()

	var walkTree func(string, float64) map[string]float64
	walkTree = func(tier string, replicaCount float64) map[string]float64 {
		if strings.Count(tier, ";") == 3 {
			replicaCount = math.Min(1.0, replicaCount)
		}
		mr := make(map[string]float64)
		mr[tier] = replicaCount
		if subTiers, ok := tier2Children[tier]; ok {
			for _, subtier := range subTiers {
				subMax := math.Ceil(replicaCount / float64(len(subTiers)))
				for k, v := range walkTree(subtier, subMax) {
					mr[k] = v
				}
			}
		}
		return mr
	}
	return walkTree("", b.Replicas)
}

// buildWantedReplicasByTier returns a map of tier -> replicanths for all tiers in the ring based on unique as possible (full dispersion) with respect to their weights and device counts.
func (b *RingBuilder) buildWantedReplicasByTier() (map[string]float64, error) {
	weightedReplicas, err := b.buildWeightedReplicasByTier()
	if err != nil {
		return nil, err
	}
	dispersedReplicas := make(map[string]minMax)
	for t, r := range weightedReplicas {
		dispersedReplicas[t] = minMax{
			min: math.Floor(r),
			max: math.Ceil(r),
		}
	}

	// Watch out for device limited tiers
	numDevices := make(map[string]int)
	numDevices[""] = 0
	for next, dev := devIterator(b.Devs); dev != nil; dev = next() {
		if dev.Weight <= 0.0 {
			continue
		}
		for _, t := range dev.tiers {
			numDevices[t] += 1
		}
		numDevices[""] += 1
	}

	tier2Children := b.buildTier2Children()
	wantedReplicas := make(map[string]float64)

	var placeReplicas func(string, float64) error
	placeReplicas = func(tier string, replicanths float64) error {
		if replicanths > float64(numDevices[tier]) {
			return errors.New(fmt.Sprintf("More replicanths (%f) than devices (%d) in tier (%s)\n", replicanths, numDevices[tier], tier))
		}
		wantedReplicas[tier] = replicanths
		if len(tier2Children[tier]) == 0 {
			return nil
		}
		subTiers := make([]string, len(tier2Children[tier]))
		copy(subTiers, tier2Children[tier])
		sort.Strings(subTiers)
		toPlace := make(map[string]float64)
		remaining := replicanths
		tiersToSpread := make([]string, len(subTiers))
		copy(tiersToSpread, subTiers)
		deviceLimited := false

		for true {
			sum := 0.0
			for _, t := range tiersToSpread {
				sum += weightedReplicas[t]
			}
			relWeight := remaining / sum
			for _, t := range tiersToSpread {
				replicas := toPlace[t] + (weightedReplicas[t] * relWeight)
				if replicas < dispersedReplicas[t].min {
					replicas = dispersedReplicas[t].min
				} else if replicas > dispersedReplicas[t].max && !deviceLimited {
					replicas = dispersedReplicas[t].max
				}
				if replicas > float64(numDevices[t]) {
					replicas = float64(numDevices[t])
				}
				toPlace[t] = replicas
			}
			remaining = replicanths
			for _, v := range toPlace {
				remaining -= v
			}
			if remaining < -1e-10 {
				tiersToSpread = tiersToSpread[:0]
				for _, t := range subTiers {
					if toPlace[t] > dispersedReplicas[t].min {
						tiersToSpread = append(tiersToSpread, t)
					}
				}
			} else if remaining > 1e-10 {
				tiersToSpread = tiersToSpread[:0]
				for _, t := range subTiers {
					if float64(numDevices[t]) > toPlace[t] && toPlace[t] < dispersedReplicas[t].max {
						tiersToSpread = append(tiersToSpread, t)
					}
				}
				if len(tiersToSpread) == 0 {
					deviceLimited = true
					tiersToSpread = tiersToSpread[:0]
					for _, t := range subTiers {
						if toPlace[t] < float64(numDevices[t]) {
							tiersToSpread = append(tiersToSpread, t)
						}
					}
				}
			} else {
				// Remaining is "empty"
				break
			}

		}
		for _, t := range subTiers {
			b.debug(fmt.Sprintf("Planning %f on %s", toPlace[t], t))
			err := placeReplicas(t, toPlace[t])
			if err != nil {
				return err
			}
		}
		return nil
	}

	// Place all replicas in the cluster tier
	if err := placeReplicas("", b.Replicas); err != nil {
		return nil, err
	}

	// Test that the for every level of the tier that the sum of weightedReplicas is very close to the total number of replicas for the ring
	replicasAtTier := [4]float64{0.0, 0.0, 0.0, 0.0}
	for t, w := range wantedReplicas {
		if t != "" {
			replicasAtTier[strings.Count(t, ";")] += w
		}
	}
	for t, r := range replicasAtTier {
		if math.Abs(b.Replicas-r) > 1e-10 {
			return nil, errors.New(fmt.Sprintf("%f != %f at tier %d", r, b.Replicas, t))
		}
	}

	return wantedReplicas, nil
}

// GetRequiredOverload returns the minimum overload value required to make the ring maximally dispersed.
//
// The required overload is the largest percentage change of any single device from its weighted replicanth to its wanted replicanth (note: under weighted devices have a negative percentage of change) to achieve dispersion - that is to say a single device that must be overloaded by 5% is worse than 5 devices in a single tier overloaded by 1%.
func (b *RingBuilder) GetRequiredOverload(weighted map[string]float64, wanted map[string]float64) (float64, error) {
	maxOverload := 0.0

	for next, dev := devIterator(b.Devs); dev != nil; dev = next() {
		tier := b.tiersForDev(dev)[3]
		if dev.Weight <= 0.0 {
			if w, ok := wanted[tier]; !ok || w <= 0.0 {
				continue
			} else {
				return 0.0, errors.New(fmt.Sprintf("Device %s has zero weight and should not want any replicas\n", tier))
			}
		}
		required := (wanted[tier] - weighted[tier]) / weighted[tier]
		b.debug(fmt.Sprintf("%s wants %f and is weighted for %f so therefore requires %f overload", tier, wanted[tier], weighted[tier], required))
		if required > maxOverload {
			maxOverload = required
		}
	}
	return maxOverload, nil
}

// buildTartgetReplicasByTier builds a map of <tier> => <targetReplicas> account for device weights, unique as possible dispersion and overload.
//
//   <tier> - a tuple, describing each tier in the ring topology
//   <targetReplicas> - a float, the target replicas at the tier
func (b *RingBuilder) buildTargetReplicasByTier() (map[string]float64, error) {
	weightedReplicas, err := b.buildWeightedReplicasByTier()
	if err != nil {
		return nil, err
	}
	wantedReplicas, err := b.buildWantedReplicasByTier()
	if err != nil {
		return nil, err
	}
	maxOverload, err := b.GetRequiredOverload(weightedReplicas, wantedReplicas)
	if err != nil {
		return nil, err
	}

	var overload float64
	if maxOverload <= 0.0 {
		return wantedReplicas, nil
	} else {
		overload = math.Min(b.Overload, maxOverload)
	}
	b.debug(fmt.Sprintf("Using effective overload of %f", overload))
	targetReplicas := make(map[string]float64)
	for tier, weighted := range weightedReplicas {
		m := (wantedReplicas[tier] - weighted) / maxOverload
		targetReplicas[tier] = m*overload + weighted
	}

	// Test that the for every level of the tier that the sum of weightedReplicas is very close to the total number of replicas for the ring
	replicasAtTier := [4]float64{0.0, 0.0, 0.0, 0.0}
	for t, w := range targetReplicas {
		if t != "" {
			replicasAtTier[strings.Count(t, ";")] += w
		}
	}
	for t, r := range replicasAtTier {
		if math.Abs(b.Replicas-r) > 1e-10 {
			return nil, errors.New(fmt.Sprintf("%f != %f at tier %d", r, b.Replicas, t))
		}
	}

	return targetReplicas, nil
}

// buildReplicaPlan wraps buildTargetReplicasByTier to include pre-calculated min and max values for each tier
func (b *RingBuilder) buildReplicaPlan() (map[string]replicaPlan, error) {
	targetReplicas, err := b.buildTargetReplicasByTier()
	if err != nil {
		return nil, err
	}
	plan := make(map[string]replicaPlan)
	for t, r := range targetReplicas {
		plan[t] = replicaPlan{
			min:    math.Floor(r + 1e-10),
			target: r,
			max:    math.Ceil(r - 1e-10),
		}
	}
	return plan, nil
}

// tiersForDev returns an array of strings representing the tiers for a given device in assending order by length.
func (b *RingBuilder) tiersForDev(dev *RingBuilderDevice) [4]string {
	t1 := dev.Region
	t2 := dev.Zone
	t3 := dev.Ip
	t4 := dev.Id
	tiers := [4]string{fmt.Sprintf("%d", t1), fmt.Sprintf("%d;%d", t1, t2), fmt.Sprintf("%d;%d;%s", t1, t2, t3), fmt.Sprintf("%d;%d;%s;%d", t1, t2, t3, t4)}
	return tiers
}

// buildTierTree constructs the tier tree from the zone layout.
//
// The tier tree is a map that maps tiers to their child tiers.
//
// An example tier tree could look like:
//
// { "":["1", "2"], "1":["1;1","1;2"], "2":["2:1"], "1;1":["1;1;192.168.101.1", "1;1;192.168.101.2"]...}
func (b *RingBuilder) buildTierTree(devs []*RingBuilderDevice) map[string][]string {
	tier2Children := make(map[string][]string)
	for next, dev := devIterator(devs); dev != nil; dev = next() {
		for _, tier := range b.tiersForDev(dev) {
			parts := strings.Split(tier, ";")
			if len(parts) == 1 {
				found := false
				for _, t := range tier2Children[""] {
					if t == tier {
						found = true
					}
				}
				if !found {
					tier2Children[""] = append(tier2Children[""], tier)
				}
			} else {
				t := strings.Join(parts[0:len(parts)-1], ";")
				found := false
				for _, t2 := range tier2Children[t] {
					if t2 == tier {
						found = true
					}
				}
				if !found {
					tier2Children[t] = append(tier2Children[t], tier)
				}
			}
		}
	}

	return tier2Children
}

// setPartsWanted sets the partsWanted key for each of the devices to the number of partitions the device wants based on its relative weight.  This key is used to sort the devices according to "most wanted" during rebalancing to best distribute partitions.  A negative partsWanted indicates the device is "overweight" and wishes to give partitions away if possible.
func (b *RingBuilder) setPartsWanted(repPlan map[string]replicaPlan) error {
	tier2Children := b.buildTier2Children()

	partsByTier := make(map[string]int)

	var placeParts func(string, int)
	placeParts = func(tier string, parts int) {
		partsByTier[tier] = parts
		subTiers := make([]string, len(tier2Children[tier]))
		copy(subTiers, tier2Children[tier])
		if len(subTiers) == 0 {
			return
		}
		sort.Strings(subTiers)
		toPlace := make(map[string]int)
		for _, t := range subTiers {
			toPlace[t] = int(math.Min(float64(parts), math.Floor(repPlan[t].target*float64(b.Parts))))
			parts -= toPlace[t]
		}

		// If there are some parts left over, just throw them about
		sort.Slice(subTiers, func(i, j int) bool { return repPlan[subTiers[i]].target < repPlan[subTiers[j]].target })
		i := 0
		for parts > 0 {
			if i > len(subTiers) {
				i = 0
			}
			toPlace[subTiers[i]] += 1
			parts -= 1
			i++
		}

		for t, p := range toPlace {
			placeParts(t, p)
		}
	}

	totalParts := int(b.Replicas * float64(b.Parts))
	placeParts("", totalParts)

	// Test that the for every level the sum of partsByTier should be totalParts for the ring
	partsAtTier := [4]int{0, 0, 0, 0}
	for t, p := range partsByTier {
		if t != "" {
			partsAtTier[strings.Count(t, ";")] += p
		}
	}
	for t, p := range partsAtTier {
		if p != totalParts {
			return errors.New(fmt.Sprintf("%d != %d at tier %d", p, totalParts, t))
		}
	}

	for next, dev := devIterator(b.Devs); dev != nil; dev = next() {
		if dev.Weight <= 0.0 {
			// With no weight, that means we wish to "drain" the device.  So we set the partsWanted to a really large negative number to indicate its strong desire to give up everything it has
			dev.PartsWanted = int64(-float64(b.Parts) * b.Replicas)
		} else {
			tier := b.tiersForDev(dev)[3]
			dev.PartsWanted = int64(partsByTier[tier]) - dev.Parts
		}
	}

	return nil
}

// adjustReplica2Part2DevSize makes sure the lengths of the arrays in replica2Part2Dev are correct for the currend value of Replicas and updates the mapping of partition -> [replicas] that need assignment
//
// Example:
//   PartPower = 8
//   Replicas = 2.25
//
// replica2Part2Dev will contain 3 arrays: the first 2 of length 256 (2**8), and the last of length 64 (0.25 * 2**8).
func (b *RingBuilder) adjustReplica2Part2DevSize(toAssign map[uint][]uint) {
	wholeReplicas, fractionalReplicas := math.Modf(b.Replicas)
	removedParts := 0
	newParts := 0

	desiredLengths := make([]int, int(wholeReplicas))
	for i := range desiredLengths {
		desiredLengths[i] = b.Parts
	}
	if fractionalReplicas > 0.0 {
		desiredLengths = append(desiredLengths, b.Parts*int(fractionalReplicas))
	}

	// If we crossed an integer threshold (say, 4.1 -> 4), we'll have a partial extra replica clinging on here.  Clean up and such extra stuff.
	if len(b.replica2Part2Dev) > len(desiredLengths) {
		for _, part2Dev := range b.replica2Part2Dev[len(desiredLengths):] {
			for _, devId := range part2Dev {
				b.Devs[devId].Parts -= 1
				removedParts -= 1
			}
		}
		b.replica2Part2Dev = b.replica2Part2Dev[:len(desiredLengths)]
	}

	for replica, desiredLength := range desiredLengths {
		if replica < len(b.replica2Part2Dev) {
			part2Dev := b.replica2Part2Dev[replica]
			if len(part2Dev) < desiredLength {
				// Not leng enough: needs to be extended and the newly added pieces assigned to devices
				for part := len(part2Dev); part < desiredLength; part++ {
					toAssign[uint(part)] = append(toAssign[uint(part)], uint(replica))
					b.replica2Part2Dev[replica] = append(b.replica2Part2Dev[replica], NONE_DEV)
					newParts += 1
				}
			} else if len(part2Dev) > desiredLength {
				// Too long: truncate this mapping
				for part := desiredLength; part < len(part2Dev); part++ {
					b.Devs[part2Dev[part]].Parts -= 1
					removedParts -= 1
					b.replica2Part2Dev[replica] = part2Dev[:desiredLength]
				}
			}
		} else {
			// Maping not present at all: make one up and assign all of it
			for part := 0; part < desiredLength; part++ {
				toAssign[uint(part)] = append(toAssign[uint(part)], uint(replica))
				newParts += 1
			}
			extra := make([]uint, desiredLength)
			for i := 0; i < len(extra); i++ {
				extra[i] = NONE_DEV
			}
			b.replica2Part2Dev = append(b.replica2Part2Dev, extra)
		}
	}
	b.debug(fmt.Sprintf("%d new parts and %d removed parts from replica-count change.", newParts, removedParts))
}

// gatherPartsFromFailedDevices updates the map of partition -> replicas to be reassigned from removed devices.
func (b *RingBuilder) gatherPartsFromFailedDevices(assignParts map[uint][]uint) int {
	// First we gather partitions from removed devices.  Since removed devices usually indicate device failures, we have no choice but to reassing these partitions.  However, we mark them as moves so later choices will skip other replicas of the same partition if possible.
	if len(b.removedDevs) > 0 {
		devsWithParts := make([]uint, 0, len(b.Devs))
		for _, dev := range b.removedDevs {
			if dev.Parts > 0 {
				devsWithParts = append(devsWithParts, uint(dev.Id))
			}
		}
		if len(devsWithParts) > 0 {
			for replica, part2Dev := range b.replica2Part2Dev {
				for part, devId := range part2Dev {
					for _, d := range devsWithParts {
						if devId == d {
							b.replica2Part2Dev[replica][part] = NONE_DEV
							b.setPartMoved(uint(part))
							assignParts[uint(part)] = append(assignParts[uint(part)], uint(replica))
							b.debug(fmt.Sprintf("Gathers %d/%d from dev %d [dev removed]", part, replica, devId))
							break
						}
					}
				}
			}

		}
		for _, dev := range b.removedDevs {
			b.debug(fmt.Sprintf("Removing dev %d", dev.Id))
			b.Devs[dev.Id] = nil
		}
		removedDevs := len(b.removedDevs)
		b.removedDevs = b.removedDevs[:0]
		return removedDevs
	}
	return 0
}

// devsForPart returns a slice of devices for a specified partition.
//
// It deliberately includes duplicates.
func (b *RingBuilder) devsForPart(part int) []*RingBuilderDevice {
	devs := make([]*RingBuilderDevice, 0, len(b.Devs))
	for _, part2Dev := range b.replica2Part2Dev {
		if part >= len(part2Dev) {
			continue
		}
		devId := part2Dev[part]
		if devId == NONE_DEV {
			continue
		}
		devs = append(devs, b.Devs[devId])
	}
	return devs
}

// replicasForPart returns a list of replicas for a specified partition.
func (b *RingBuilder) replicasForPart(part int) []int {
	replicas := make([]int, 0, int(b.Replicas))
	for replica, part2Dev := range b.replica2Part2Dev {
		if part < len(part2Dev) {
			replicas = append(replicas, replica)
		}
	}
	return replicas
}

// gatherPartsForDispersion updates the map of partition -> [replicas] to be reassigned from insufficiently-far-apart replicas.
func (b *RingBuilder) gatherPartsForDispersion(assignParts map[uint][]uint, repPlan map[string]replicaPlan) {
	// Gather partitions that are "at risk" because they aren't currently sufficiently spread out across the cluster.
	for part := 0; part < b.Parts; part++ {
		if !b.canPartMove(uint(part)) {
			continue
		}
		// First, add up the count of replicas at each tier for each partition
		replicasAtTier := make(map[string]int)
		devs4Part := b.devsForPart(part)
		for _, dev := range devs4Part {
			for _, tier := range dev.tiers {
				replicasAtTier[tier] += 1
			}
		}

		// Look for partitions not yet spread out enough.
		rfp := b.replicasForPart(part)
		undispersedDevReplicas := make([]devReplica, 0, len(rfp))
		for _, replica := range rfp {
			devId := b.replica2Part2Dev[replica][part]
			if devId == NONE_DEV {
				continue
			}
			dev := b.Devs[devId]
			for _, tier := range dev.tiers {
				if float64(replicasAtTier[tier]) > repPlan[tier].max {
					undispersedDevReplicas = append(undispersedDevReplicas, devReplica{dev, replica})
					break
				}
			}
		}
		if len(undispersedDevReplicas) == 0 {
			continue
		}
		sort.Slice(undispersedDevReplicas, func(i, j int) bool {
			return undispersedDevReplicas[i].dev.PartsWanted < undispersedDevReplicas[j].dev.PartsWanted
		})
		for _, dr := range undispersedDevReplicas {
			// The min part hour check is ignored if and only if a device has more than one replica of a part assigned to it
			dev := dr.dev
			replica := dr.replica
			if !b.canPartMove(uint(part)) && !(replicasAtTier[dev.tiers[3]] > 1) {
				continue
			}
			dev.PartsWanted += 1
			dev.Parts -= 1
			assignParts[uint(part)] = append(assignParts[uint(part)], uint(replica))
			b.debug(fmt.Sprintf("Gathered %d/%d from dev %d [dispersion]", part, replica, dev.Id))
			b.replica2Part2Dev[replica][part] = NONE_DEV
			for _, tier := range dev.tiers {
				replicasAtTier[tier] -= 1
				b.setPartMoved(uint(part))
			}
		}
	}
}

// gatherPartsForBalanceCanDisperse updates the map of partition -> [replicas] to be reassigned from overweight drives where the replicas can be better dispersed to another failure domain.
func (b *RingBuilder) gatherPartsForBalanceCanDisperse(assignParts map[uint][]uint, start int, repPlan map[string]replicaPlan) {
	for offset := 0; offset < b.Parts; offset++ {
		part := (start + offset) % b.Parts
		if !b.canPartMove(uint(part)) {
			continue
		}
		rfp := b.replicasForPart(part)
		overweightDevReplica := make([]devReplica, 0, len(rfp))
		replicasAtTier := make(map[string]int)
		for _, replica := range rfp {
			devId := b.replica2Part2Dev[replica][part]
			if devId == NONE_DEV {
				continue
			}
			dev := b.Devs[devId]
			for _, tier := range dev.tiers {
				replicasAtTier[tier] += 1
			}
			if dev.PartsWanted < 0 {
				overweightDevReplica = append(overweightDevReplica, devReplica{dev, replica})
			}
		}
		if len(overweightDevReplica) == 0 {
			continue
		}

		sort.Slice(overweightDevReplica, func(i, j int) bool {
			return overweightDevReplica[i].dev.PartsWanted < overweightDevReplica[j].dev.PartsWanted
		})
	OUTER:
		for _, dt := range overweightDevReplica {
			dev := dt.dev
			replica := dt.replica
			for _, tier := range dev.tiers {
				if repPlan[tier].min <= float64(replicasAtTier[tier]) && float64(replicasAtTier[tier]) < repPlan[tier].max {
					continue OUTER
				}
			}

			// This is the most overweightDevice holding a replica of this part that can shed it according to the plan
			dev.PartsWanted += 1
			dev.Parts -= 1
			assignParts[uint(part)] = append(assignParts[uint(part)], uint(replica))
			b.debug(fmt.Sprintf("Gathered %d/%d from dev %d [weight disperse]", part, replica, dev.Id))
			b.replica2Part2Dev[replica][part] = NONE_DEV
			for _, tier := range dev.tiers {
				replicasAtTier[tier] -= 1
			}
			b.setPartMoved(uint(part))
			break
		}
	}
}

// gatherPartsForBalanceForced updates the map of partition -> [replicas] to be reassigned from overweight drives without restriction.  Parts gathered from this method may be placed back onto devices that are no better (or worse) than the device from which they are gathered.
//
// This method allows devices to flop around enough to unlock replicas that would have otherwise potentially been locked because of dispersion -- it should be used as a last resort.
func (b *RingBuilder) gatherPartsForBalanceForced(assignParts map[uint][]uint, start int) {
	for offset := 0; offset < b.Parts; offset++ {
		part := (start + offset) % b.Parts
		if !b.canPartMove(uint(part)) {
			continue
		}
		rfp := b.replicasForPart(part)
		overweightDevReplica := make([]devReplica, 0, len(rfp))
		for _, replica := range rfp {
			devId := b.replica2Part2Dev[replica][part]
			if devId == NONE_DEV {
				continue
			}
			dev := b.Devs[devId]
			if dev.PartsWanted < 0 {
				overweightDevReplica = append(overweightDevReplica, devReplica{dev, replica})
			}
		}
		if len(overweightDevReplica) == 0 {
			continue
		}

		sort.Slice(overweightDevReplica, func(i, j int) bool {
			return overweightDevReplica[i].dev.PartsWanted < overweightDevReplica[j].dev.PartsWanted
		})

		dr := overweightDevReplica[0]
		dev := dr.dev
		replica := dr.replica
		// This is the most overweightDevice holding a replica of this part we don't know where it's going to end up -- but we'll pick it up and hope for the best
		dev.PartsWanted += 1
		dev.Parts -= 1
		assignParts[uint(part)] = append(assignParts[uint(part)], uint(replica))
		b.debug(fmt.Sprintf("Gathered %d/%d from dev %d [weight forced]", part, replica, dev.Id))
		b.replica2Part2Dev[replica][part] = NONE_DEV
		b.setPartMoved(uint(part))

	}
}

// gatherPartsForBalance gathers parts that look like they should move for balance reasons.
func (b *RingBuilder) gatherPartsForBalance(assignParts map[uint][]uint, repPlan map[string]replicaPlan) {
	// Pick a random starting point on the other side of the ring
	quarterTurn := b.Parts / 4
	randomHalf := rand.Intn(b.Parts / 2)
	start := (b.lastPartGatherStart + quarterTurn + randomHalf) % b.Parts
	b.debug(fmt.Sprintf("Gather start is %d (Last start was %d)", start, b.lastPartGatherStart))
	b.lastPartGatherStart = start

	b.gatherPartsForBalanceCanDisperse(assignParts, start, repPlan)
	if len(assignParts) == 0 {
		b.gatherPartsForBalanceForced(assignParts, start)
	}
}

// For an existing ring data set, partitions are reassigned similar to the initial assignment.
//
// The devices are ordered by how many partitions they still want and keps in that order throughout the process.
//
// The gathered partitions are iterated through, assigning them to devices according to the "most wanted" while keeping the replicas as "far apart" as possible.
//
// Two different regions are considred the fartheres-apart things, followed by zones, then different ip within a zone; the least-far-apart things are different devices with the same ip in the same zone.
func (b *RingBuilder) reassignParts(reassignParts []partReplicas, repPlan map[string]replicaPlan) error {
	partsAvailableInTier := make(map[string]int)
	availableDevs := make([]*RingBuilderDevice, 0, len(b.Devs))
	for next, dev := devIterator(b.Devs); dev != nil; dev = next() {
		wanted := int(math.Max(float64(dev.PartsWanted), 0))
		for _, tier := range dev.tiers {
			partsAvailableInTier[tier] += wanted
		}
		if dev.Weight > 0.0 {
			availableDevs = append(availableDevs, dev)
		}
	}
	sort.Slice(availableDevs, func(i, j int) bool {
		if availableDevs[i].PartsWanted == availableDevs[j].PartsWanted {
			return rand.Int() < rand.Int()
		} else {
			return availableDevs[i].PartsWanted < availableDevs[j].PartsWanted
		}
	})
	tier2Devs := make(map[string][]*RingBuilderDevice)
	maxTierDepth := 0
	for i := range availableDevs {
		for _, tier := range availableDevs[i].tiers {
			tier2Devs[tier] = append(tier2Devs[tier], availableDevs[i])
			tierDepth := strings.Count(tier, ";") + 1
			if tierDepth > maxTierDepth {
				maxTierDepth = tierDepth
			}
		}
	}
	tier2ChildrenSets := b.buildTierTree(availableDevs)
	tier2Children := make(map[string][]string)
	tiersList := []string{""}
	depth := 1
	for depth <= maxTierDepth {
		newTiersList := make([]string, 0)
		for _, tier := range tiersList {
			childTiers := tier2ChildrenSets[tier]
			// NOTE: Not sure if the following is in the same vein as the original
			sort.Slice(childTiers, func(i, j int) bool {
				iLen := len(tier2Devs[childTiers[i]])
				jLen := len(tier2Devs[childTiers[j]])
				if tier2Devs[childTiers[i]][iLen-1].PartsWanted == tier2Devs[childTiers[j]][jLen-1].PartsWanted {
					return rand.Int() < rand.Int()
				} else {
					return tier2Devs[childTiers[i]][iLen-1].PartsWanted < tier2Devs[childTiers[j]][jLen-1].PartsWanted
				}
			})
			tier2Children[tier] = childTiers
			for _, tier := range childTiers {
				newTiersList = append(newTiersList, tier)
			}
		}
		tiersList = newTiersList
		depth += 1
	}

	// Shuffle reassignParts
	for i := range reassignParts {
		j := rand.Intn(i + 1)
		reassignParts[i], reassignParts[j] = reassignParts[j], reassignParts[i]
	}
	for _, rp := range reassignParts {
		part := rp.part
		replaceReplicas := rp.replicas
		// always update partMoves for minPartHours
		b.lastPartMoves[part] = 0
		// Count up where these replicas be
		replicasAtTier := make(map[string]int)
		for _, dev := range b.devsForPart(int(part)) {
			for _, tier := range dev.tiers {
				replicasAtTier[tier] += 1
			}
		}

		// Shuffle the replicas
		for i := range replaceReplicas {
			j := rand.Intn(i + 1)
			replaceReplicas[i], replaceReplicas[j] = replaceReplicas[j], replaceReplicas[i]
		}
		for _, replica := range replaceReplicas {
			// Find a new home for this replicas
			tier := ""
			depth := 1
			for depth <= maxTierDepth {
				candidates := make([]string, 0)
				for _, t := range tier2Children[tier] {
					if float64(replicasAtTier[t]) < repPlan[t].max {
						candidates = append(candidates, t)
					}
				}
				if len(candidates) == 0 {
					data := make(map[string]string)
					for _, t := range tier2Children[tier] {
						data[t] = fmt.Sprintf("%v - MAX: %v", replicasAtTier[t], repPlan[t].max)
					}
					return errors.New(fmt.Sprintf("no home for %d/%d %+v", part, replica, data))
				}

				// Shuffle the candidates
				for i := range candidates {
					j := rand.Intn(i + 1)
					candidates[i], candidates[j] = candidates[j], candidates[i]
				}
				curTier := candidates[0]
				for _, t := range candidates {
					if partsAvailableInTier[t] > partsAvailableInTier[curTier] {
						curTier = t
					}
				}
				tier = curTier
				depth += 1
			}

			dev := tier2Devs[tier][len(tier2Devs[tier])-1]
			dev.PartsWanted -= 1
			dev.Parts += 1
			for _, t := range dev.tiers {
				partsAvailableInTier[t] -= 1
				replicasAtTier[t] += 1
			}
			b.replica2Part2Dev[replica][part] = uint(dev.Id)
			b.debug(fmt.Sprintf("Placed %d/%d onto dev %d", part, replica, dev.Id))
		}
	}
	return nil
}

// buildBalancePerDev builds a map of deviceId -> balance where balance represents the percentage difference from the desired amount of partitions a given device wants and the amount it has.
func (b *RingBuilder) buildBalancePerDev() map[int64]float64 {
	weightOfOnePart := b.WeightOfOnePart()
	balancePerDev := make(map[int64]float64)
	balance := 0.0
	for next, dev := devIterator(b.Devs); dev != nil; dev = next() {
		if dev.Weight == 0.0 {
			if dev.Parts > 0 {
				// If a device has no weight but has partitions, then its overage is considered "infinity" and there always the wors possible.  We sho MAX_BALANCE for convenience.
				balance = MAX_BALANCE
			} else {
				balance = 0.0
			}
		} else {
			balance = 100.0*float64(dev.Parts)/(dev.Weight*weightOfOnePart) - 100.0
		}
		balancePerDev[dev.Id] = balance
	}
	return balancePerDev
}

// GetBalance gets the balance of the ring.
//
// The balance value is the highest percentage of the desired amount of partitions a given device wants.  For instance, if the "worst" device wants (based on tis weight relative to the sum of all devices' weights) 123 partitions and it has 124 partitions, the balance value would be 0.83 (1 extra / 123 wanted * 100)
func (b *RingBuilder) GetBalance() float64 {
	balancePerDev := b.buildBalancePerDev()
	balance := 0.0
	for _, b := range balancePerDev {
		balance = math.Max(math.Abs(b), balance)
	}
	return balance
}

// Rebalance rebalances the ring.
//
// This is the main work function of the builder, as it will assign and reassing partitions to devices in the ring based on weights, distinct zones, recent reassignments, etc.
//
// The proces doesn't always perfectly assign partitions (that'd take a lot more analysis and therefore a lot more time.  Because of this, it keeps rebalancing until the device skew (number of partitions a device wants compared to what it has) gets below 1% or doesn't change by more than 1% (only happens with a ring that can't be balanced no matter what).
func (b *RingBuilder) Rebalance() (int, float64, int, error) {
	numDevices := 0
	for next, dev := devIterator(b.Devs); dev != nil; dev = next() {
		// NOTE: original ringbuilder added a tiers thing, not sure if needed yet
		dev.tiers = b.tiersForDev(dev)
		if dev.Weight > 0 {
			numDevices += 1
		}
	}
	if float64(numDevices) < b.Replicas {
		return 0, 0.0, 0, errors.New(fmt.Sprintf("Replica count of %f requires more than %d devices.", b.Replicas, numDevices))
	}

	oldReplica2Part2Dev := make([][]uint, len(b.replica2Part2Dev))
	for i := range b.replica2Part2Dev {
		oldReplica2Part2Dev[i] = make([]uint, len(b.replica2Part2Dev[i]))
		copy(oldReplica2Part2Dev[i], b.replica2Part2Dev[i])
	}

	b.updateLastPartMoves()

	repPlan, err := b.buildReplicaPlan()
	if err != nil {
		return 0, 0.0, 0, err
	}
	if err := b.setPartsWanted(repPlan); err != nil {
		return 0, 0.0, 0, err
	}

	assignParts := make(map[uint][]uint)
	// gather parts from replica count adjustments
	b.adjustReplica2Part2DevSize(assignParts)
	// Gather parts for failed devices
	removedDevs := b.gatherPartsFromFailedDevices(assignParts)
	// Gather parts for dispersion
	b.gatherPartsForDispersion(assignParts, repPlan)

	finishStatus := "Unable to finish"
	// Gather a few times or until we achive the plan
	gatherCount := 0
GATHER:
	for gatherCount = 0; gatherCount < MAX_BALANCE_GATHER_COUNT; gatherCount++ {
		b.gatherPartsForBalance(assignParts, repPlan)
		if len(assignParts) == 0 {
			break
		}
		assignPartsList := make([]partReplicas, 0, len(assignParts))
		for p, r := range assignParts {
			assignPartsList = append(assignPartsList, partReplicas{p, r})
		}
		// range on a map is already random, so we don't have to shuffle assignPartsList
		// reset assignParts map for next iteration
		// NOTE: It isn't clear to me why this is done...
		assignParts = make(map[uint][]uint)

		numPartReplicas := 0
		for _, pr := range assignPartsList {
			numPartReplicas += len(pr.replicas)
		}
		b.debug(fmt.Sprintf("Gathered %d parts.", numPartReplicas))
		b.reassignParts(assignPartsList, repPlan)
		b.debug(fmt.Sprintf("Assigned %d parts.", numPartReplicas))

		for next, dev := devIterator(b.Devs); dev != nil; dev = next() {
			if dev.PartsWanted >= 0 {
				continue GATHER
			}
		}
		finishStatus = "Finished"
		break
	}
	b.debug(fmt.Sprintf("%s rebalance plan after %d attempts.", finishStatus, gatherCount+1))
	b.DevsChanged = false
	b.Version += 1
	// NOTE/TODO?: Dispersion graph isn't currently implemented

	// Figure out how many parts moved
	changedParts := 0
	for repId := range b.replica2Part2Dev {
		for partId, devId := range b.replica2Part2Dev[repId] {
			if partId >= len(oldReplica2Part2Dev[repId]) || devId != oldReplica2Part2Dev[repId][partId] {
				changedParts += 1
			}
		}
	}

	return changedParts, b.GetBalance(), removedDevs, nil
}

func (b *RingBuilder) SearchDevs(region, zone int64, ip string, port int64, repIp string, repPort int64, device string, weight float64, meta string) []*RingBuilderDevice {
	foundDevs := make([]*RingBuilderDevice, 0, len(b.Devs))
	for next, dev := devIterator(b.Devs); dev != nil; dev = next() {
		if region >= 0 && region != dev.Region {
			continue
		}
		if zone >= 0 && zone != dev.Zone {
			continue
		}
		if ip != "" && ip != dev.Ip {
			continue
		}
		if port >= 0 && port != dev.Port {
			continue
		}
		if repIp != "" && repIp != dev.ReplicationIp {
			continue
		}
		if repPort >= 0 && repPort != dev.ReplicationPort {
			continue
		}
		if device != "" && device != dev.Device {
			continue
		}
		if weight >= 0.0 && weight != dev.Weight {
			continue
		}
		if meta != "" && strings.Contains(dev.Meta, meta) {
			continue
		}
		foundDevs = append(foundDevs, dev)
	}
	return foundDevs
}

func (b *RingBuilder) UpdateDevInfo(devId int64, newIp string, newPort int64, newRepIp string, newRepPort int64, newDevice, newMeta string) error {
	// first check to make sure another device doesn't have the ip/port/device
	if newIp == "" {
		newIp = b.Devs[devId].Ip
	}
	if newPort < 0 {
		newPort = b.Devs[devId].Port
	}
	if newDevice == "" {
		newDevice = b.Devs[devId].Device
	}
	for next, dev := devIterator(b.Devs); dev != nil; dev = next() {
		if dev.Id != devId && dev.Ip == newIp && dev.Port == newPort && dev.Device == newDevice {
			return errors.New(fmt.Sprintf("Device id %d already uses %s:%d:/%s.", dev.Id, newIp, newPort, newDevice))
		}
	}
	b.Devs[devId].Ip = newIp
	b.Devs[devId].Port = newPort
	b.Devs[devId].Device = newDevice
	if newRepIp != "" {
		b.Devs[devId].ReplicationIp = newRepIp
	}
	if newRepPort >= 0 {
		b.Devs[devId].ReplicationPort = newRepPort
	}
	if newMeta != "" {
		b.Devs[devId].Meta = newMeta
	}
	return nil
}

// ChangeMinPartHours changes the value used to decide if a given partition can be moved again.  This restriction is to give the overall system enough time to settl a partition to its new location before moving it to yet another location.  While no data would be lost if a partition is moved several times quickly, it could make the data unreachable for a short period of time.
//
// This should be set to at least the average full partition replication time.  Starting it at 24 hours and then lowering it to what the replicator reprots as the longest partition cycle is best.
func (b *RingBuilder) ChangeMinPartHours(minPartHours int) {
	b.MinPartHours = minPartHours
}

// SetReplicas sets the number of replicas in this ring.
//
// If the new replica count is sufficiently different that replica2Part2Dev will change size, sets devsChanged.  This is so tools can know to write out the new ring rather than bailing out due to lack of balance change.
func (b *RingBuilder) SetReplicas(newReplicaCount float64) {
	oldSlotsUsed := int(float64(b.Parts) * b.Replicas)
	newSlotsUsed := int(float64(b.Parts) * newReplicaCount)
	if oldSlotsUsed != newSlotsUsed {
		b.DevsChanged = true
	}
	b.Replicas = newReplicaCount
}

func (b *RingBuilder) GetRing() *hashRing {
	data := ringData{
		ReplicaCount: int(b.Replicas),
		PartShift:    uint64(32 - b.PartPower),
	}
	for i, dev := range b.Devs {
		if dev != nil {
			data.Devs = append(data.Devs, Device{
				Id:              int(b.Devs[i].Id),
				Device:          b.Devs[i].Device,
				Ip:              b.Devs[i].Ip,
				Meta:            b.Devs[i].Meta,
				Port:            int(b.Devs[i].Port),
				Region:          int(b.Devs[i].Region),
				ReplicationIp:   b.Devs[i].ReplicationIp,
				ReplicationPort: int(b.Devs[i].ReplicationPort),
				Weight:          b.Devs[i].Weight,
				Zone:            int(b.Devs[i].Zone),
			})
		}
	}
	data.replica2part2devId = make([][]uint16, len(b.replica2Part2Dev))
	for i := range b.replica2Part2Dev {
		data.replica2part2devId[i] = make([]uint16, len(b.replica2Part2Dev[i]))
		for j := range b.replica2Part2Dev[i] {
			data.replica2part2devId[i][j] = uint16(b.replica2Part2Dev[i][j])
		}
	}
	r := &hashRing{}
	r.data.Store(&data)

	return r
}

// AddDev adds a device to the ring
//
// Note: This will not reblance the ring immediately as you may want to make multiple changes for a single rebalance
//
// Returns the id of the device
func (b *RingBuilder) AddDev(dev *RingBuilderDevice) (int64, error) {
	// Check for duplicat ip:port/devs
	for i := 0; i < len(b.Devs); i++ {
		if b.Devs[i] != nil {
			if b.Devs[i].Ip == dev.Ip && b.Devs[i].Port == dev.Port && b.Devs[i].Device == dev.Device {
				return 0, errors.New(fmt.Sprintf("Device %d already uses %s:%d/%s.", i, dev.Ip, dev.Port, dev.Device))
			}
		}
	}
	if dev.Id < 0 {
		// find the next id
		id := int64(0)
		for i := 0; i < len(b.Devs); i++ {
			if b.Devs[i] == nil {
				break
			}
			id += 1
		}
		dev.Id = id
	}
	if dev.Id < int64(len(b.Devs)) && b.Devs[dev.Id] != nil {
		return -1, errors.New(fmt.Sprintf("Duplicate device id: %d", dev.Id))
	}
	for dev.Id >= int64(len(b.Devs)) {
		// We need to fill in the gaps
		b.Devs = append(b.Devs, nil)
	}
	dev.Parts = 0
	b.Devs[dev.Id] = dev
	b.DevsChanged = true
	b.Version += 1
	return dev.Id, nil
}

// Command line functions

// CreateRing creates a ring builder file.
//   builderpath must include the filename of the the builder to create.
//   A backup folder will also be created in the back with a backup of the original builder.
func CreateRing(builderPath string, partPower int, replicas float64, minPartHours int, debug bool) error {
	builder, err := NewRingBuilder(partPower, replicas, minPartHours, debug)
	if err != nil {
		return err
	}
	backupPath := path.Join(path.Dir(builderPath), "backups")
	err = os.Mkdir(backupPath, 0777)
	if err != nil {
		e := err.(*os.PathError)
		if e.Err != syscall.EEXIST {
			return err
		}
	}
	err = builder.Save(path.Join(backupPath, fmt.Sprintf("%d.%s", time.Now().Unix(), path.Base(builderPath))))
	if err != nil {
		return err
	}
	err = builder.Save(builderPath)
	if err != nil {
		return err
	}
	return nil
}

// Rebalance attempts to rebalance the ring by reassigning partitions that haven't been recently reassigned.
func Rebalance(builderPath string, debug bool) error {
	builder, err := NewRingBuilderFromFile(builderPath, debug)
	if err != nil {
		return err
	}
	changed, balance, removed, err := builder.Rebalance()
	if err != nil {
		return err
	}
	fmt.Printf("Changed: %d Balance: %f Removed: %d\n", changed, balance, removed)
	err = builder.Save(builderPath)
	if err != nil {
		return err
	}
	ringFile := strings.TrimSuffix(builderPath, ".builder") + ".ring.gz"
	r := builder.GetRing()
	err = r.Save(ringFile)
	if err != nil {
		return err
	}
	return nil
}

// AddDevice adds a device to the builder filer
//   builderpath must include the filename of the builder file.
//   Returns the id of the device in the ring.
func AddDevice(builderPath string, id, region, zone int64, ip string, port int64, replicationIp string, replicationPort int64, device string, weight float64, debug bool) (int64, error) {
	builder, err := NewRingBuilderFromFile(builderPath, debug)
	if err != nil {
		return -1, err
	}
	dev := &RingBuilderDevice{
		Id:              id,
		Region:          region,
		Zone:            zone,
		Ip:              ip,
		Port:            port,
		ReplicationIp:   replicationIp,
		ReplicationPort: replicationPort,
		Device:          device,
		Weight:          weight,
	}
	id, err = builder.AddDev(dev)
	if err != nil {
		return -1, err
	}
	err = builder.Save(builderPath)
	if err != nil {
		return -1, err
	}
	return id, nil
}

func Search(builderPath string, region, zone int64, ip string, port int64, repIp string, repPort int64, device string, weight float64, meta string) ([]*RingBuilderDevice, error) {
	builder, err := NewRingBuilderFromFile(builderPath, false)
	if err != nil {
		return nil, err
	}
	devs := builder.SearchDevs(region, zone, ip, port, repIp, repPort, device, weight, meta)
	return devs, nil
}

func SetWeight(builderPath string, devs []*RingBuilderDevice, weight float64) error {
	builder, err := NewRingBuilderFromFile(builderPath, false)
	if err != nil {
		return err
	}
	for _, dev := range devs {
		err := builder.SetDevWeight(dev.Id, weight)
		if err != nil {
			return err
		}
	}
	builder.Save(builderPath)
	return nil
}

func RemoveDevs(builderPath string, devs []*RingBuilderDevice) error {
	builder, err := NewRingBuilderFromFile(builderPath, false)
	if err != nil {
		return err
	}
	for _, dev := range devs {
		builder.RemoveDev(dev.Id)
	}
	builder.Save(builderPath)
	return nil
}

func SetInfo(builderPath string, devs []*RingBuilderDevice, newIp string, newPort int64, newRepIp string, newRepPort int64, newDevice, newMeta string) error {
	builder, err := NewRingBuilderFromFile(builderPath, false)
	if err != nil {
		return err
	}
	for _, dev := range devs {
		err := builder.UpdateDevInfo(dev.Id, newIp, newPort, newRepIp, newRepPort, newDevice, newMeta)
		if err != nil {
			return err
		}
	}
	builder.Save(builderPath)
	return nil
}

func WriteRing(builderPath string) error {
	builder, err := NewRingBuilderFromFile(builderPath, false)
	if err != nil {
		return err
	}
	r := builder.GetRing()
	if len(builder.replica2Part2Dev) == 0 {
		if len(builder.Devs) > 0 {
			fmt.Println("Warning: Writing a ring with no partiton assignments but with devices;  did you forget to run \"rebalance\"?")
		} else {
			fmt.Println("Warning: Writing an empty ring.")
		}
	}
	ringFile := strings.TrimSuffix(builderPath, ".builder") + ".ring.gz"
	if err := r.Save(ringFile); err != nil {
		return err
	}
	return nil
}
