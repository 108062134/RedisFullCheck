package common

import (
	"bytes"
	"fmt"
	"strconv"

	redigo "github.com/gomodule/redigo/redis"
)

type ClusterNodeInfo struct {
	Id          string
	Address     string
	Flags       string
	Master      string
	PingSent    string
	PongRecv    string
	ConfigEpoch string
	LinkStat    string
	Slot        string
}

func ParseKeyspace(content []byte) (map[int32]int64, error) {
	if bytes.HasPrefix(content, []byte("# Keyspace")) == false {
		return nil, fmt.Errorf("invalid info Keyspace: %s", string(content))
	}

	lines := bytes.Split(content, []byte("\n"))
	reply := make(map[int32]int64)
	for _, line := range lines {
		line = bytes.TrimSpace(line)
		if bytes.HasPrefix(line, []byte("db")) == true {
			// line "db0:keys=18,expires=0,avg_ttl=0"
			items := bytes.SplitN(line, []byte(":"), 2)
			if len(items) != 2 {
				return nil, fmt.Errorf("invalid keyspace entry: missing separator")
			}

			db, err := strconv.ParseInt(string(items[0][2:]), 10, 32)
			if err != nil {
				return nil, fmt.Errorf("invalid Redis DB index: %w", err)
			}
			if db < 0 {
				return nil, fmt.Errorf("negative Redis DB index: %d", db)
			}

			nums := bytes.SplitN(items[1], []byte(","), 2)
			if !bytes.HasPrefix(nums[0], []byte("keys=")) {
				return nil, fmt.Errorf("invalid keyspace entry: missing keys count")
			}
			keysNum, err := strconv.ParseInt(string(nums[0][5:]), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid Redis key count: %w", err)
			}
			if keysNum < 0 {
				return nil, fmt.Errorf("negative Redis key count: %d", keysNum)
			}

			reply[int32(db)] = keysNum
		} // end true
	} // end for
	return reply, nil
}

/*
 * 10.1.1.1:21331> cluster nodes
 * d49a4c7b516b8da222d46a0a589b77f381285977 10.1.1.1:21333@31333 master - 0 1557996786000 3 connected 10923-16383
 * f23ba7be501b2dcd4d6eeabd2d25551513e5c186 10.1.1.1:21336@31336 slave d49a4c7b516b8da222d46a0a589b77f381285977 0 1557996785000 6 connected
 * 75fffcd521738606a919607a7ddd52bcd6d65aa8 10.1.1.1:21331@31331 myself,master - 0 1557996784000 1 connected 0-5460
 * da3dd51bb9cb5803d99942e0f875bc5f36dc3d10 10.1.1.1:21332@31332 master - 0 1557996786260 2 connected 5461-10922
 * eff4e654d3cc361a8ec63640812e394a8deac3d6 10.1.1.1:21335@31335 slave da3dd51bb9cb5803d99942e0f875bc5f36dc3d10 0 1557996787261 5 connected
 * 486e081f8d47968df6a7e43ef9d3ba93b77d03b2 10.1.1.1:21334@31334 slave 75fffcd521738606a919607a7ddd52bcd6d65aa8 0 1557996785258 4 connected
 */
func ParseClusterNode(content []byte) []*ClusterNodeInfo {
	lines := bytes.Split(content, []byte("\n"))
	ret := make([]*ClusterNodeInfo, 0, len(lines))
	for _, line := range lines {
		if bytes.Compare(line, []byte{}) == 0 {
			continue
		}

		items := bytes.Fields(line)
		if len(items) < 8 {
			continue
		}

		address := bytes.Split(items[1], []byte{'@'})
		role := parseClusterNodeRole(items[2])
		var slot string
		if len(items) > 8 {
			slot = string(items[8])
		}
		ret = append(ret, &ClusterNodeInfo{
			Id:          string(items[0]),
			Address:     string(address[0]),
			Flags:       role,
			Master:      string(items[3]),
			PingSent:    string(items[4]),
			PongRecv:    string(items[5]),
			ConfigEpoch: string(items[6]),
			LinkStat:    string(items[7]),
			Slot:        slot,
		})
	}
	return ret
}

func parseClusterNodeRole(flags []byte) string {
	parts := bytes.Split(flags, []byte{','})
	for _, part := range parts {
		if bytes.Equal(part, []byte(TypeMaster)) || bytes.Equal(part, []byte(TypeSlave)) {
			return string(part)
		}
	}

	if len(parts) > 0 {
		return string(parts[0])
	}
	return ""
}

// needMaster: true(master), false(slave)
func ClusterNodeChoose(input []*ClusterNodeInfo, role string) []*ClusterNodeInfo {
	ret := make([]*ClusterNodeInfo, 0, len(input))
	for _, ele := range input {
		if ele.Flags == TypeMaster && role == TypeMaster ||
			ele.Flags == TypeSlave && role == TypeSlave ||
			role == TypeAll {
			ret = append(ret, ele)
		}
	}
	return ret
}

// return id list if  choose == "id", otherwise address
func GetAllClusterNode(client redigo.Conn, role string, choose string) ([]string, error) {
	ret, err := client.Do("cluster", "nodes")
	if err != nil {
		return nil, err
	}

	nodeList := ParseClusterNode(ret.([]byte))
	nodeListChoose := ClusterNodeChoose(nodeList, role)

	result := make([]string, 0, len(nodeListChoose))
	for _, ele := range nodeListChoose {
		if choose == "id" {
			result = append(result, ele.Id)
		} else {
			result = append(result, ele.Address)
		}
	}

	return result, nil
}

// compare two unordered list. return true means equal.
func CompareUnorderedList(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	if len(a) == 0 {
		return true
	}

	setA := map[string]struct{}{}

	for _, x := range a {
		setA[x] = struct{}{}
	}

	for _, x := range b {
		if _, ok := setA[x]; !ok {
			return false
		}
	}

	return true
}
