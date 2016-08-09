package protocol

type JobQueue []*Master

func (jq JobQueue) Len() int {
	return len(jq)
}

func (jq JobQueue) Less(i, j int) bool {
	a := jq[i].GetParent()
	b := jq[j].GetParent()
	if a == b { //a == b == ""
		return jq[i].GetID() < jq[j].GetID()
	}
	if a == "" {
		return false
	}
	if b == "" {
		return true
	}
	return a < b
}

func (jq JobQueue) Swap(i, j int) {
	jq[i], jq[j] = jq[j], jq[i]
}

func (jq *JobQueue) Push(x interface{}) {
	*jq = append(*jq, x.(*Master))
}

func (jq *JobQueue) Pop() interface{} {
	if len(*jq) <= 0 {
		return nil
	}
	old := *jq
	n := len(old)
	master := old[n-1]
	*jq = old[0: n-1]
	return master
}
