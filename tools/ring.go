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

package tools

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"time"

	"github.com/gholt/brimtext"
	"github.com/troubling/hummingbird/common/ring"
)

func PrintDevs(devs []*ring.RingBuilderDevice) {
	data := make([][]string, 0)
	data = append(data, []string{"ID", "REGION", "ZONE", "IP ADDRESS", "PORT", "REPLICATION IP", "REPLICATION PORT", "NAME", "WEIGHT", "PARTITIONS", "META"})
	data = append(data, nil)
	for _, dev := range devs {
		if dev != nil {
			data = append(data, []string{strconv.FormatInt(dev.Id, 10), strconv.FormatInt(dev.Region, 10), strconv.FormatInt(dev.Zone, 10), dev.Ip, strconv.FormatInt(dev.Port, 10), dev.ReplicationIp, strconv.FormatInt(dev.ReplicationPort, 10), dev.Device, strconv.FormatFloat(dev.Weight, 'f', -1, 64), strconv.FormatInt(dev.Parts, 10), dev.Meta})
		}
	}
	fmt.Println(brimtext.Align(data, brimtext.NewSimpleAlignOptions()))
}

func RingBuildCmd(flags *flag.FlagSet) {
	args := flags.Args()
	if len(args) < 1 || args[0] == "help" {
		flags.Usage()
		return
	}
	debug := flags.Lookup("debug").Value.String() == "true"
	pth := args[0]
	cmd := ""
	if len(args) == 1 {
		cmd = "info"
	} else {
		cmd = args[1]
	}
	switch cmd {
	case "create":
		if len(args) < 5 {
			flags.Usage()
			return
		}
		partPower, err := strconv.Atoi(args[2])
		if err != nil {
			fmt.Println(err)
			return
		}
		replicas, err := strconv.ParseFloat(args[3], 64)
		if err != nil {
			fmt.Println(err)
			return
		}
		minPartHours, err := strconv.Atoi(args[4])
		if err != nil {
			fmt.Println(err)
			return
		}
		err = ring.CreateRing(pth, partPower, replicas, minPartHours, debug)
		if err != nil {
			fmt.Println(err)
			return
		}

	case "rebalance":
		if len(args) < 1 {
			flags.Usage()
			return
		}
		err := ring.Rebalance(pth, debug)
		if err != nil {
			fmt.Println(err)
		}
		return

	case "pretend_min_part_hours_passed":
		ring.PretendMinPartHoursPassed(pth)
		return

	case "search":
		searchFlags := flag.NewFlagSet("search", flag.ExitOnError)
		region := searchFlags.Int64("region", -1, "Device region.")
		zone := searchFlags.Int64("zone", -1, "Device zone.")
		ip := searchFlags.String("ip", "", "Device ip address.")
		port := searchFlags.Int64("port", -1, "Device port.")
		repIp := searchFlags.String("replication-ip", "", "Device replication address.")
		repPort := searchFlags.Int64("replication-port", -1, "Device replication port.")
		device := searchFlags.String("device", "", "Device name.")
		weight := searchFlags.Float64("weight", -1.0, "Device weight.")
		meta := searchFlags.String("meta", "", "Metadata.")
		if err := searchFlags.Parse(args[2:]); err != nil {
			fmt.Println(err)
		}
		devs, err := ring.Search(pth, *region, *zone, *ip, *port, *repIp, *repPort, *device, *weight, *meta)
		if err != nil {
			fmt.Println(err)
			return
		}
		if len(devs) == 0 {
			fmt.Println("No matching devices found.")
			return
		}
		PrintDevs(devs)
		return

	case "set_info":
		changeFlags := flag.NewFlagSet("search", flag.ExitOnError)
		region := changeFlags.Int64("region", -1, "Device region.")
		zone := changeFlags.Int64("zone", -1, "Device zone.")
		ip := changeFlags.String("ip", "", "Device ip address.")
		port := changeFlags.Int64("port", -1, "Device port.")
		repIp := changeFlags.String("replication-ip", "", "Device replication address.")
		repPort := changeFlags.Int64("replication-port", -1, "Device replication port.")
		device := changeFlags.String("device", "", "Device name.")
		weight := changeFlags.Float64("weight", -1.0, "Device weight.")
		meta := changeFlags.String("meta", "", "Metadata.")
		yes := changeFlags.Bool("yes", false, "Force yes.")
		newIp := changeFlags.String("change-ip", "", "New ip address.")
		newPort := changeFlags.Int64("change-port", -1, "New port.")
		newRepIp := changeFlags.String("change-replication-ip", "", "New replication ip address.")
		newRepPort := changeFlags.Int64("change-replication-port", -1, "New replication port.")
		newDevice := changeFlags.String("change-device", "", "New device name.")
		newMeta := changeFlags.String("change-meta", "", "New meta data.")
		if err := changeFlags.Parse(args[2:]); err != nil {
			fmt.Println(err)
			return
		}
		devs, err := ring.Search(pth, *region, *zone, *ip, *port, *repIp, *repPort, *device, *weight, *meta)
		if err != nil {
			fmt.Println(err)
			return
		}
		if len(devs) == 0 {
			fmt.Println("No matching devices found.")
			return
		} else {
			reader := bufio.NewReader(os.Stdin)
			fmt.Println("Search matched the following devices:")
			PrintDevs(devs)
			if !*yes {
				fmt.Printf("Are you sure you want to update the info for these %d devices (y/n)? ", len(devs))
				resp, err := reader.ReadString('\n')
				if err != nil {
					fmt.Println(err)
					return
				}
				if resp[0] != 'y' && resp[0] != 'Y' {
					fmt.Println("No devices updated.")
					return
				}
			}
			err := ring.SetInfo(pth, devs, *newIp, *newPort, *newRepIp, *newRepPort, *newDevice, *newMeta)
			if err != nil {
				fmt.Println(err)
			} else {
				fmt.Println("Devices updated successfully.")
			}
		}

	case "set_weight":
		weightFlags := flag.NewFlagSet("set_weight", flag.ExitOnError)
		region := weightFlags.Int64("region", -1, "Device region.")
		zone := weightFlags.Int64("zone", -1, "Device zone.")
		ip := weightFlags.String("ip", "", "Device ip address.")
		port := weightFlags.Int64("port", -1, "Device port.")
		repIp := weightFlags.String("replication-ip", "", "Device replication address.")
		repPort := weightFlags.Int64("replication-port", -1, "Device replication port.")
		device := weightFlags.String("device", "", "Device name.")
		weight := weightFlags.Float64("weight", -1.0, "Device weight.")
		meta := weightFlags.String("meta", "", "Metadata.")
		yes := weightFlags.Bool("yes", false, "Force yes.")
		if err := weightFlags.Parse(args[2:]); err != nil {
			fmt.Println(err)
			return
		}
		args := weightFlags.Args()
		if len(args) < 1 {
			weightFlags.Usage()
			return
		}
		newWeight, err := strconv.ParseFloat(args[0], 64)
		if err != nil {
			fmt.Println(err)
			return
		}
		devs, err := ring.Search(pth, *region, *zone, *ip, *port, *repIp, *repPort, *device, *weight, *meta)
		if err != nil {
			fmt.Println(err)
			return
		}
		if len(devs) == 0 {
			fmt.Println("No matching devices found.")
			return
		} else {
			reader := bufio.NewReader(os.Stdin)
			fmt.Println("Search matched the following devices:")
			PrintDevs(devs)
			if !*yes {
				fmt.Printf("Are you sure you want to update the weight to %.2f for these %d devices (y/n)? ", newWeight, len(devs))
				resp, err := reader.ReadString('\n')
				if err != nil {
					fmt.Println(err)
					return
				}
				if resp[0] != 'y' && resp[0] != 'Y' {
					fmt.Println("No devices updated.")
					return
				}
			}
			err := ring.SetWeight(pth, devs, newWeight)
			if err != nil {
				fmt.Println(err)
			} else {
				fmt.Println("Weight updated successfully.")
			}
		}

	case "remove":
		weightFlags := flag.NewFlagSet("set_weight", flag.ExitOnError)
		region := weightFlags.Int64("region", -1, "Device region.")
		zone := weightFlags.Int64("zone", -1, "Device zone.")
		ip := weightFlags.String("ip", "", "Device ip address.")
		port := weightFlags.Int64("port", -1, "Device port.")
		repIp := weightFlags.String("replication-ip", "", "Device replication address.")
		repPort := weightFlags.Int64("replication-port", -1, "Device replication port.")
		device := weightFlags.String("device", "", "Device name.")
		weight := weightFlags.Float64("weight", -1.0, "Device weight.")
		meta := weightFlags.String("meta", "", "Metadata.")
		yes := weightFlags.Bool("yes", false, "Force yes.")
		if err := weightFlags.Parse(args[2:]); err != nil {
			fmt.Println(err)
			return
		}
		devs, err := ring.Search(pth, *region, *zone, *ip, *port, *repIp, *repPort, *device, *weight, *meta)
		if err != nil {
			fmt.Println(err)
			return
		}
		if len(devs) == 0 {
			fmt.Println("No matching devices found.")
			return
		} else {
			reader := bufio.NewReader(os.Stdin)
			fmt.Println("Search matched the following devices:")
			PrintDevs(devs)
			if !*yes {
				fmt.Printf("Are you sure you want to remove these %d devices (y/n)? ", len(devs))
				resp, err := reader.ReadString('\n')
				if err != nil {
					fmt.Println(err)
					return
				}
				if resp[0] != 'y' && resp[0] != 'Y' {
					fmt.Println("No devices removed.")
					return
				}
			}
			err := ring.RemoveDevs(pth, devs)
			if err != nil {
				fmt.Println(err)
			} else {
				fmt.Println("Devices removed successfully.")
			}
		}
	case "write_ring":
		if err := ring.WriteRing(pth); err != nil {
			fmt.Println(err)
		}
	case "add":
		// TODO: Add config option version of add function
		// TODO: Add support for multiple adds in a single command
		var err error
		var region, zone, port, replicationPort int64
		var ip, replicationIp, device string
		var weight float64

		if len(args) < 4 {
			flags.Usage()
			return
		}
		deviceStr := args[2]
		rx := regexp.MustCompile(`^(?:r(?P<region>\d+))?z(?P<zone>\d+)-(?P<ip>[\d\.]+):(?P<port>\d+)(?:R(?P<replication_ip>[\d\.]+):(?P<replication_port>\d+))?\/(?P<device>[^_]+)(?:_(?P<metadata>.+))?$`)
		matches := rx.FindAllStringSubmatch(deviceStr, -1)
		if len(matches) == 0 {
			flags.Usage()
			return
		}
		if matches[0][1] != "" {
			region, err = strconv.ParseInt(matches[0][1], 0, 64)
			if err != nil {
				fmt.Println(err)
				return
			}
		} else {
			region = 0
		}
		zone, err = strconv.ParseInt(matches[0][2], 0, 64)
		if err != nil {
			fmt.Println(err)
			return
		}
		ip = matches[0][3]
		port, err = strconv.ParseInt(matches[0][4], 0, 64)
		if err != nil {
			fmt.Println(err)
			return
		}

		if matches[0][5] != "" {
			replicationIp = matches[0][5]
			replicationPort, err = strconv.ParseInt(matches[0][6], 0, 64)
			if err != nil {
				fmt.Println(err)
				return
			}
		} else {
			replicationIp = ""
			replicationPort = 0
		}
		device = matches[0][7]
		weight, err = strconv.ParseFloat(args[3], 64)
		if err != nil {
			fmt.Println(err)
			return
		}
		id, err := ring.AddDevice(pth, -1, region, zone, ip, port, replicationIp, replicationPort, device, weight, debug)
		if err != nil {
			fmt.Println(err)
			return
		} else {
			fmt.Printf("Device %s with %.2f weight added with id %d\n", device, weight, id)
		}
	case "load":
		builder, err := ring.NewRingBuilderFromFile(pth, debug)
		fmt.Printf("%+v\n", builder)
		if err != nil {
			fmt.Println(err)
			return
		}
	case "info":
		// Show info about the ring
		builder, err := ring.NewRingBuilderFromFile(pth, debug)
		if err != nil {
			fmt.Println(err)
			return
		}
		regions := 0
		zones := 0
		devCount := 0
		balance := 0.0
		if len(builder.Devs) > 0 {
			regionSet := make(map[int64]bool)
			zoneSet := make(map[string]bool)
			for _, dev := range builder.Devs {
				if dev != nil {
					regionSet[dev.Region] = true
					zoneSet[fmt.Sprintf("%d,%d", dev.Region, dev.Zone)] = true
					devCount += 1
				}
			}
			regions = len(regionSet)
			zones = len(zoneSet)
			balance = builder.GetBalance()
		}
		fmt.Printf("%s, build version %d, %d partitions, %.6f replicas, %d regions, %d zones, %d devices, %.02f balance\n", pth, builder.Version, builder.Parts, builder.Replicas, regions, zones, devCount, balance)
		fmt.Printf("The minimum number of hours before a partition can be reassigned is %v (%v remaining)\n", builder.MinPartHours, time.Duration(builder.MinPartSecondsLeft())*time.Second)
		fmt.Printf("The overload factor is %0.2f%% (%.6f)\n", builder.Overload*100, builder.Overload)

		// Compare ring file against builder file
		// TODO: Figure out how to do ring comparisons

		PrintDevs(builder.Devs)

	case "analyze":
		epsilon := func(a, b float64) float64 {
			// returns the ratio of the difference between two numbers to their average.
			// this gives you a vague idea of how close two numbers are.
			if a > b {
				return (a - b) / ((a + b) / 2.0)
			}
			return (b - a) / ((a + b) / 2.0)
		}
		builder, err := ring.NewRingBuilderFromFile(pth, debug)
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Printf("Analyzing %s...\n", pth)
		ring := builder.GetRing()
		fmt.Println("Total Partitions: ", ring.PartitionCount())
		replicas := int(ring.ReplicaCount())
		devs := ring.AllDevices()
		fmt.Println("Total Devices: ", len(devs))
		totalWeight := float64(0)
		devPartitions := make([]map[uint64]bool, len(devs))
		for i, dev := range devs {
			if dev != nil {
				totalWeight += dev.Weight
				devPartitions[i] = make(map[uint64]bool)
			}
		}
		partCounts := make([]int64, len(devs))
		for part := uint64(0); part < ring.PartitionCount(); part++ {
			for _, node := range ring.GetNodes(part) {
				devPartitions[node.Id][part] = true
				partCounts[node.Id]++
			}
		}

		for i, dev := range devs {
			if dev != nil {
				want := (dev.Weight / totalWeight) * float64(ring.PartitionCount()) * float64(replicas)
				if epsilon(float64(partCounts[i]), want) > 0.02 {
					fmt.Println("Device", dev.Id, "partition count >1% off its want:", partCounts[i], "vs", want)
				}
			}
		}

		totalPairings := int64(0)
		for i, dev1 := range devs {
			if dev1 != nil {
				for _, dev2 := range devs[i:] {
					if dev2 != nil && dev1.Id != dev2.Id {
						totalPairings += partCounts[dev1.Id] * partCounts[dev2.Id]
					}
				}
			}
		}
		totalSharesRequired := ring.PartitionCount() * (ring.ReplicaCount() * ((ring.ReplicaCount() - 1) / 2))
		if ring.ReplicaCount() == 2 {
			totalSharesRequired = ring.PartitionCount()
		}
		for i, dev1 := range devs {
			if dev1 != nil {
				for _, dev2 := range devs[i:] {
					if dev2 != nil && dev1.Id != dev2.Id {
						shouldShare := float64(partCounts[dev1.Id]*partCounts[dev2.Id]) *
							(float64(totalSharesRequired) / float64(totalPairings))
						shared := float64(0)
						for part := range devPartitions[dev1.Id] {
							if devPartitions[dev2.Id][part] {
								shared++
							}
						}
						if epsilon(shared, shouldShare) > 0.02 {
							fmt.Println(dev1.Id, "and", dev2.Id, "should share", shouldShare, "partitions, but share", shared)
						}
					}
				}
			}
		}

		regions := make(map[int]bool)
		zones := make(map[int]bool)
		ips := make(map[string]bool)
		devices := make(map[string]bool)
		primaryCounts := make(map[int][]int, len(devs))
		for _, dev := range devs {
			if dev != nil {
				regions[dev.Region] = true
				ips[dev.Ip] = true
				zones[dev.Zone] = true
				devices[fmt.Sprintf("%s:%s", dev.Ip, dev.Device)] = true
				primaryCounts[dev.Id] = make([]int, replicas)
			}
		}
		for part := uint64(0); part < ring.PartitionCount(); part++ {
			partRegions := make(map[int]bool)
			partZones := make(map[int]bool)
			partIps := make(map[string]bool)
			partDevices := make(map[string]bool)
			for _, dev := range ring.GetNodes(part) {
				partRegions[dev.Region] = true
				partZones[dev.Zone] = true
				partIps[dev.Ip] = true
				partDevices[fmt.Sprintf("%s:%s", dev.Ip, dev.Device)] = true
			}
			if len(partRegions) < len(regions) && len(partRegions) < replicas {
				fmt.Printf("Partition %d uses %d/%d available regions.\n", part, len(partRegions), len(regions))
			}
			if len(partZones) < len(zones) && len(partZones) < replicas {
				fmt.Printf("Partition %d uses %d/%d available zones.\n", part, len(partZones), len(zones))
			}
			if len(partIps) < len(ips) && len(partIps) < replicas {
				fmt.Printf("Partition %d uses %d/%d available IPs.\n", part, len(partIps), len(ips))
			}
			if len(partDevices) < len(devices) && len(partDevices) < replicas {
				fmt.Printf("Partition %d uses %d/%d available devices.", part, len(partDevices), len(devices))
			}

			for i, node := range ring.GetNodes(part) {
				primaryCounts[node.Id][i]++
			}
		}
		for _, dev := range devs {
			if dev != nil {
				expectedParts := float64(ring.PartitionCount()) * float64(dev.Weight) / totalWeight
				for i, parts := range primaryCounts[dev.Id] {
					if epsilon(float64(parts), float64(expectedParts)) > 0.02 {
						fmt.Println(dev.Id, "is primary number", i, "for", parts, "partitions, but that should be", int(expectedParts))
					}
				}
			}
		}
		fmt.Println("Done!")

	case "validate":
		err := ring.Validate(pth)
		if err != nil {
			fmt.Println(err)
		}
	default:
		fmt.Printf("Unknown command: %s\n", cmd)
		flags.Usage()
	}

}
