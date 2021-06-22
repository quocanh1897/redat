package redat

import (
	"fmt"
	ui "github.com/gizak/termui/v3"
	"github.com/gizak/termui/v3/widgets"
	"github.com/spf13/viper"
	"log"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"
)

var (
	CLUSTER_IPS   []string
	CLUSTER_PORTS []string
)

type Cluster struct {
	ClusterIps   []string
	ClusterPorts []string
}

type MasterSlave struct {
	isDown bool
	master Node
	slaves []Node
}
type Node struct {
	status       string
	id           string
	host         string
	port         string
	role         int64
	label        string
	connection   string
	slotRange    string
	reciprocalId string
	timestamp    string
	lastUpdated  string
	oldRole      string
	keysCount    string
	datasetSize  string
}

func loadConfig() {
	viper.SetConfigName("./config")
	viper.AddConfigPath("./")
	viper.SetConfigType("yml")
	err := viper.ReadInConfig()
	if err != nil {
		fmt.Println("fatal error config file: default \n", err)
	}
	var config Cluster
	err = viper.UnmarshalKey("cluster", &config)
	if err != nil {
		fmt.Println("fatal error unmarshal", err)
	}
	CLUSTER_IPS = config.ClusterIps
	CLUSTER_PORTS = config.ClusterPorts

}

func executeCommand(command string, extra string) string {
	cmd := exec.Command("bash", "-c", command)
	out, _ := cmd.CombinedOutput()
	output := string(out[:])
	if extra != "" {
		if output == "1" {
			output = " "
		}
	}
	return output
}
func getCurrentHost(ts int64) string {
	num := ts % int64(len(CLUSTER_PORTS))
	return CLUSTER_IPS[num] + ":" + CLUSTER_PORTS[num]
}

func getClusterInfo(ts int64) string {
	output := "Connection refused"
	num := ts % int64(len(CLUSTER_PORTS))
	for strings.Contains(output, "Connection refused") {
		cmd := "redis-cli -h " + CLUSTER_IPS[num] + " -p " + CLUSTER_PORTS[num] + " --raw CLUSTER INFO | grep -v _stats_"
		output = executeCommand(cmd, "GET cluster nodes info")
		num = ts % int64(len(CLUSTER_PORTS))
	}
	return output
}

func getMemUsage(cluster []MasterSlave) [][]string {
	res := [][]string{{"Node", "Keys count", "Dataset size (byte)"}}

	if cluster == nil {
		return res
	}
	output := "Connection refused"
	for strings.Contains(output, "Connection refused") {
		for pairIdx, pair := range cluster {
			ip := pair.master.host
			if len(ip) < 5 {
				continue
			}
			// get port op ip
			index := 0
			for i, clusterIp := range CLUSTER_IPS {
				if strings.EqualFold(clusterIp, ip) {
					index = i
				}
			}
			port := CLUSTER_PORTS[index]
			cmd := "redis-cli -h " + ip + " -p " + port + " --raw  memory stats | grep -E \"keys.count|dataset.bytes\" -A 1"
			output = executeCommand(cmd, "GET Memory usage")
			lines := strings.Split(output, "\n")

			if len(lines) > 4 {
				row := []string{cluster[pairIdx].master.host + ":" + cluster[pairIdx].master.port, lines[1], lines[4]}
				res = append(res, row)
			}
		}

	}
	return res
}

//o...rd host:port myself,master        -      	0 			  1624206016000 3  connected    10923-16383
//d...91 host:port slave                6...00 	0 			  1624213621000 10 connected
//7...a4 :0@0      slave,fail,noaddr    6...00 	1624013796577 1624013794000 10 disconnected
//3..70  host:port master,fail 			- 		1624214569565 1624214567000 2 disconnected
func marshalToNode(inp string) Node {
	fields := strings.Split(inp, " ")

	addr := strings.Split(fields[1], ":")
	role := 0
	label := "[S]"
	if strings.Contains(fields[2], "master") {
		role = 1
		label = "[M]"
	}
	slotRange := "nil"
	if len(fields) > 8 {
		slotRange = fields[8]
	}
	lu, _ := strconv.ParseInt(fields[4], 10, 64)
	ts, _ := strconv.ParseInt(fields[5], 10, 64)
	conn, _ := strconv.ParseInt(fields[6], 10, 64)

	stt := "OK"
	oldRole := ""
	if strings.Contains(inp, "fail") || strings.Contains(inp, "noaddr") {
		stt = "FAIL"
		label = "[!!]"
		oldRole = "slave"
		if role == 1 {
			oldRole = "master"
		}
		role = 0
	}
	return Node{
		id:           fields[0],
		host:         addr[0],
		port:         addr[1],
		role:         int64(role),
		oldRole:      oldRole,
		label:        label,
		status:       stt,
		slotRange:    slotRange,
		timestamp:    time.Unix(ts/1000, 0).Format("2006/01/02 15:04"),
		connection:   fmt.Sprintf("%d %s", conn, fields[7]),
		lastUpdated:  time.Unix(lu/1000, 0).Format("2006/01/02 15:04"),
		reciprocalId: fields[3],
	}
}

func getClusterTopology(ticker int64) []MasterSlave {
	output := "Connection refused"
	num := ticker % int64(len(CLUSTER_PORTS))
	for strings.Contains(output, "Connection refused") {
		cmd := "redis-cli -h " + CLUSTER_IPS[num] + " -p " + CLUSTER_PORTS[num] + " --raw CLUSTER NODES"
		output = executeCommand(cmd, "GET cluster nodes info")
		num = ticker % int64(len(CLUSTER_PORTS))
	}
	return marshalToMasterSlave(output)
}

// By is the type of a "less" function that defines the ordering of its Planet arguments.
type By func(p1, p2 *MasterSlave) bool
type ByNode func(p1, p2 *Node) bool

// Sort is a method on the function type, By, that sorts the argument slice according to the function.
func (by By) Sort(pairs []MasterSlave) {
	ps := &pairSorter{
		pairs: pairs,
		by:    by, // The Sort method's receiver is the function (closure) that defines the sort order.
	}
	sort.Sort(ps)
}
func (by ByNode) SortNode(pairs []Node) {
	ps := &pairSorterNode{
		pairs: pairs,
		by:    by, // The Sort method's receiver is the function (closure) that defines the sort order.
	}
	sort.Sort(ps)
}

// pairSorter joins a By function and a slice of MasterSlaves to be sorted.
type pairSorter struct {
	pairs []MasterSlave
	by    func(p1, p2 *MasterSlave) bool // Closure used in the Less method.
}

// pairSorter joins a By function and a slice of MasterSlaves to be sorted.
type pairSorterNode struct {
	pairs []Node
	by    func(p1, p2 *Node) bool // Closure used in the Less method.
}

// Len is part of sort.Interface.
func (s *pairSorter) Len() int {
	return len(s.pairs)
}
func (s *pairSorterNode) Len() int {
	return len(s.pairs)
}

// Swap is part of sort.Interface.
func (s *pairSorter) Swap(i, j int) {
	s.pairs[i], s.pairs[j] = s.pairs[j], s.pairs[i]
}
func (s *pairSorterNode) Swap(i, j int) {
	s.pairs[i], s.pairs[j] = s.pairs[j], s.pairs[i]
}

// Less is part of sort.Interface. It is implemented by calling the "by" closure in the sorter.
func (s *pairSorter) Less(i, j int) bool {
	return s.by(&s.pairs[i], &s.pairs[j])
}
func (s *pairSorterNode) Less(i, j int) bool {
	return s.by(&s.pairs[i], &s.pairs[j])
}

func marshalToMasterSlave(output string) []MasterSlave {
	lines := strings.Split(output, "\n")
	var res []MasterSlave
	failPair := MasterSlave{
		master: Node{
			id: "",
		},
		isDown: true,
	}
	port := func(n1, n2 *MasterSlave) bool {
		return n1.master.port > n2.master.port
	}
	portNode := func(n1, n2 *Node) bool {
		return n1.port > n2.port
	}
	for _, line := range lines {
		if len(line) < 40 {
			continue
		}

		var pair MasterSlave
		node := marshalToNode(line)

		if node.role == 1 {
			pair.master = node

			for _, another := range lines {
				if len(another) < 40 {
					continue
				}
				anotherNode := marshalToNode(another)
				if anotherNode.reciprocalId == pair.master.id {
					pair.slaves = append(pair.slaves, anotherNode)
				}
			}
			ByNode(portNode).SortNode(pair.slaves)

			res = append(res, pair)
		}

		if node.status != "OK" && len(node.reciprocalId) < 2 {
			failPair.slaves = append(failPair.slaves, node)
		}
	}
	res = append(res, failPair)

	By(port).Sort(res)

	return res
}

func renderNode(node Node) string {
	if node.role == 1 {
		return strings.Join([]string{node.label, "[" + node.host, node.port, node.connection, node.id + "](fg:yellow)"}, " ")
	} else if node.status != "OK" {
		return strings.Join([]string{node.label, "[" + node.host, node.port, node.connection, node.id, node.oldRole + "](fg:red)"}, " ")
	}
	return strings.Join([]string{node.label, "[" + node.host, node.port, node.connection, node.id + "](fg:green)"}, " ")
}

func pairsToUIRows(pairs []MasterSlave) []interface{} {
	var uiRows []interface{}

	loading := "/"
	switch time.Now().Unix() % 3 {
	case 1:
		loading = "-"
	case 2:
		loading = "\\"
	}
	for _, pair := range pairs {
		var rows []string
		comp := widgets.NewList()
		rows = append(rows, string(ui.QUOTA_RIGHT))

		if !pair.isDown {
			comp.Title = "[" + loading + "]" + "HEALTHY"
			rows = append(rows, renderNode(pair.master))
		} else {
			comp.Title = "[" + loading + "]" + "ERROR"
		}

		for _, slave := range pair.slaves {
			rows = append(rows, renderNode(slave))
		}

		comp.Rows = rows
		comp.SelectedRowStyle = ui.NewStyle(ui.ColorClear)
		uiRows = append(uiRows, ui.NewRow(1.0/float64(len(pairs)), comp))
	}

	return uiRows
}

func reRender(grid *ui.Grid, ticker int64, clusterInfo *widgets.Paragraph, currentHost *widgets.Paragraph, memUsage *widgets.Table) {
	currentHost.Text = getCurrentHost(ticker)

	keybinding := widgets.NewParagraph()
	keybinding.Title = "Key binding"
	keybinding.Text = "[<q>](fg:red) QUIT \n[<s>](fg:red) PAUSE/RESUME \n[<r>](fg:red) CLEAR"

	topology := getClusterTopology(ticker)

	memUsage.Rows = getMemUsage(topology)
	termWidth, termHeight := ui.TerminalDimensions()
	memUsage.SetRect(termWidth*1.0/2, termHeight*3.0/9, termWidth, termHeight)
	grid.Set(
		ui.NewCol(2.0/3,
			pairsToUIRows(topology)...,
		),
		ui.NewCol(1.0/3,
			ui.NewRow(2.0/9, keybinding),
			ui.NewRow(1.0/9, currentHost),
			ui.NewRow(3.0/9, memUsage),
			ui.NewRow(3.0/9, clusterInfo),
		),
	)
	ui.Render(grid)
}

var run = true

func main() {
	loadConfig()
	if err := ui.Init(); err != nil {
		log.Fatalf("failed to initialize termui: %v", err)
	}
	defer ui.Close()

	ts := time.Now().Unix()
	clusterInfo := widgets.NewParagraph()
	clusterInfo.Title = "Cluster Info"
	clusterInfo.Text = getClusterInfo(ts)

	currentHost := widgets.NewParagraph()
	currentHost.Title = "Current host"
	currentHost.Text = getCurrentHost(ts)

	memUsage := widgets.NewTable()
	memUsage.Title = "Memory Usage"
	memUsage.Rows = [][]string{
		{"Node", "Keys count", "Dataset size (byte)"},
	}

	grid := ui.NewGrid()
	termWidth, termHeight := ui.TerminalDimensions()
	grid.SetRect(0, 0, termWidth, termHeight)

	reRender(grid, ts, clusterInfo, currentHost, memUsage)

	pause := func() {
		run = !run
		if run {
			currentHost.Title = "Current host"
		} else {
			currentHost.Title = "Current host (Pause)"
		}
		ui.Render(grid)
	}

	tickerCount := 1
	uiEvents := ui.PollEvents()
	ticker := time.NewTicker(time.Second).C
	for {
		select {
		case e := <-uiEvents:
			switch e.ID {
			case "r":
				ui.Clear()
			case "q":
				return
			case "s":
				pause()
			case "<Resize>":
				payload := e.Payload.(ui.Resize)
				grid.SetRect(0, 0, payload.Width, payload.Height)
				reRender(grid, ts, clusterInfo, currentHost, memUsage)
				ui.Clear()
				ui.Render(grid)
			}
		case <-ticker:
			if run {
				ui.Clear()
				reRender(grid, time.Now().Unix(), clusterInfo, currentHost, memUsage)
				tickerCount++
			}
		}
	}
}
