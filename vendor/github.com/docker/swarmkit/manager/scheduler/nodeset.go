package scheduler

import (
	"container/heap"
	"errors"
	"time"
	"sync"
	"net/http"
	"io/ioutil"
	"strconv"

	"github.com/docker/swarmkit/api"
	"github.com/bitly/go-simplejson"
	"github.com/datexla/go-cmdlog"
)

const (
	cpuWeight = 1.0
	memWeight = 1.0
	infWeight = 1000000000.0
)

var errNodeNotFound = errors.New("node not found in scheduler dataset")

type nodeSet struct {
	lock  sync.RWMutex
	nodes map[string]NodeInfo // map from node id to node info
}

func (ns *nodeSet) alloc(n int) {
	ns.nodes = make(map[string]NodeInfo, n)
}

// nodeInfo returns the NodeInfo struct for a given node identified by its ID.
func (ns *nodeSet) nodeInfo(nodeID string) (NodeInfo, error) {
	node, ok := ns.nodes[nodeID]
	if ok {
		return node, nil
	}
	return NodeInfo{}, errNodeNotFound
}

// addOrUpdateNode sets the number of tasks for a given node. It adds the node
// to the set if it wasn't already tracked.
func (ns *nodeSet) addOrUpdateNode(n NodeInfo) {
	if n.Tasks == nil {
		n.Tasks = make(map[string]*api.Task)
	}
	if n.DesiredRunningTasksCountByService == nil {
		n.DesiredRunningTasksCountByService = make(map[string]int)
	}
	if n.recentFailures == nil {
		n.recentFailures = make(map[string][]time.Time)
	}

	ns.nodes[n.ID] = n
}

// updateNode sets the number of tasks for a given node. It ignores the update
// if the node isn't already tracked in the set.
func (ns *nodeSet) updateNode(n NodeInfo) {
	_, ok := ns.nodes[n.ID]
	if ok {
		ns.nodes[n.ID] = n
	}
}

func (ns *nodeSet) remove(nodeID string) {
	delete(ns.nodes, nodeID)
}

type nodeMaxHeap struct {
	nodes    []NodeInfo
	lessFunc func(*NodeInfo, *NodeInfo) bool
	length   int
}

func (h nodeMaxHeap) Len() int {
	return h.length
}

func (h nodeMaxHeap) Swap(i, j int) {
	h.nodes[i], h.nodes[j] = h.nodes[j], h.nodes[i]
}

func (h nodeMaxHeap) Less(i, j int) bool {
	// reversed to make a max-heap
	return h.lessFunc(&h.nodes[j], &h.nodes[i])
}

func (h *nodeMaxHeap) Push(x interface{}) {
	h.nodes = append(h.nodes, x.(NodeInfo))
	h.length++
}

func (h *nodeMaxHeap) Pop() interface{} {
	h.length--
	// return value is never used
	return nil
}

// findBestNodes returns n nodes (or < n if fewer nodes are available) that
// rank best (lowest) according to the sorting function.
func (ns *nodeSet) findBestNodes(n int, meetsConstraints func(*NodeInfo) bool, nodeLess func(*NodeInfo, *NodeInfo) bool) []NodeInfo {
	if n == 0 {
		return []NodeInfo{}
	}

	nodeHeap := nodeMaxHeap{lessFunc: nodeLess}

	// TODO(aaronl): Is is possible to avoid checking constraints on every
	// node? Perhaps we should try to schedule with n*2 nodes that weren't
	// prescreened, and repeat the selection if there weren't enough nodes
	// meeting the constraints.
	for _, node := range ns.nodes {
		// If there are fewer then n nodes in the heap, we add this
		// node if it meets the constraints. Otherwise, the heap has
		// n nodes, and if this node is better than the worst node in
		// the heap, we replace the worst node and then fix the heap.
		if nodeHeap.Len() < n {
			if meetsConstraints(&node) {
				heap.Push(&nodeHeap, node)
			}
		} else if nodeLess(&node, &nodeHeap.nodes[0]) {
			if meetsConstraints(&node) {
				nodeHeap.nodes[0] = node
				heap.Fix(&nodeHeap, 0)
			}
		}
	}

	// Popping every element orders the nodes from best to worst. The
	// first pop gets the worst node (since this a max-heap), and puts it
	// at position n-1. Then the next pop puts the next-worst at n-2, and
	// so on.
	for nodeHeap.Len() > 0 {
		heap.Pop(&nodeHeap)
	}

	return nodeHeap.nodes
}

func (ns *nodeSet) updateAllNodeScore() error {
	url := "http://127.0.0.1:4243/nodes"

	res, err := http.Get(url)

	if err != nil {
		return errors.New("call url failed")
	}

	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return errors.New("parse response body failed")
	}

	if string(body) == "null\n" {
		return errors.New("api return null")
	}

	statsJson, err := simplejson.NewJson(body)
	if err != nil {
		return errors.New("parse json failed")
	}

	peersNum := len(statsJson.MustArray())

	wg := new(sync.WaitGroup)

	for i := 0 ; i < peersNum; i++ {
		peer := statsJson.GetIndex(i)
		ip := peer.Get("Status").Get("Addr").MustString()
		nodeId := peer.Get("ID").MustString()

		managerStatus := peer.Get("ManagerStatus").Get("Leader").MustBool()
		if managerStatus {
			ns.lock.RLock()
			nodeInfo := ns.nodes[nodeId]
			ns.lock.RUnlock()
			nodeInfo.scoreSelf = infWeight
			ns.lock.Lock()
			ns.nodes[nodeId] = nodeInfo
			ns.lock.Unlock()
			cmdlog.Write(cmdlog.ManagerInfo, "hostName: " + nodeInfo.Description.Hostname + " is a manager, neglecting calculating manager's score" + ", nodeID: " + nodeId + ", ip: " + ip, cmdlog.DefaultPathToFile)
			continue
		}

		wg.Add(1)
		go calcNodeScore(ns, nodeId, ip, wg)
	}

	wg.Wait()

	return nil
}

// func calcNodeScore(ns *nodeSet, id string, ip string,  wg *sync.WaitGroup) error {
func calcNodeScore(ns *nodeSet, id string, ip string,  wg *sync.WaitGroup) {
	// Decreasing internal counter for wait-group as soon as goroutine finishes
	defer wg.Done()
	ns.lock.RLock()
	nodeInfo := ns.nodes[id]
	ns.lock.RUnlock()
	// assume score is zero if not reach from url
	nodeInfo.scoreSelf = 0.0
	ns.lock.Lock()
	ns.nodes[id] = nodeInfo
	ns.lock.Unlock()

	// call url
	url := "http://" + ip + ":4243/containers/all/stats"
	res, err := http.Get(url)
	if err != nil {
		return
	}

	cmdlog.Write(cmdlog.Debug, "after http get: " + res.Status, cmdlog.DefaultPathToFile)

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return
	}

	statsJson, err := simplejson.NewJson(body)
	if err != nil {
		return
	}

	statsNum := len(statsJson.MustArray())

	var usedCPU float64 = 0.0
	var usedMem float64 = 0.0

	for i := 0; i < statsNum; i++ {
		stat := statsJson.GetIndex(i)

		//calculate CPU usage
		cpuPercentage := 0.0

		currCPU := stat.Get("cpu_stats").Get("cpu_usage").Get("total_usage").MustFloat64()
		prevCPU := stat.Get("precpu_stats").Get("cpu_usage").Get("total_usage").MustFloat64()
		deltaCPU := currCPU - prevCPU
		
		currSys := stat.Get("cpu_stats").Get("system_cpu_usage").MustFloat64()
		prevSys := stat.Get("precpu_stats").Get("system_cpu_usage").MustFloat64()
		deltaSys := currSys - prevSys
		
		numCores := float64(len(stat.Get("cpu_stats").Get("cpu_usage").Get("percpu_usage").MustArray()))
		
		if deltaCPU > 0.0 && deltaSys > 0.0 {
			cpuPercentage = deltaCPU / deltaSys * numCores * 100.0
		}

		usedCPU += cpuPercentage

		//calculate memory usage
		usedMem += stat.Get("memory_stats").Get("usage").MustFloat64()
	}

	totalMem := float64(nodeInfo.Description.Resources.MemoryBytes)

	cpuScore := usedCPU
	memScore := usedMem / totalMem * 100.0
	score := cpuWeight * cpuScore + memWeight * memScore

	// update score
	nodeInfo.scoreSelf = score
	ns.lock.Lock()
	ns.nodes[id] = nodeInfo
	ns.lock.Unlock()

	scoreStr := strconv.FormatFloat(score, 'f', -1, 64)
	cpuScoreStr := strconv.FormatFloat(cpuScore, 'f', -1, 64)
	memScoreStr := strconv.FormatFloat(memScore, 'f', -1, 64)
	cmdlog.Write(cmdlog.ScorePrint, "hostName: " + nodeInfo.Description.Hostname + ", score: " + scoreStr + ", cpuScore: " + cpuScoreStr + ", memScore: " + memScoreStr + ", nodeID: " + id + ", ip: " + ip, cmdlog.DefaultPathToFile)
}
