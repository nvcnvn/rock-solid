package core

func concurrency(in chan int) { // want `sans-IO package uses a channel: exchange Events and Actions with the loop`
	ch := make(chan int, 1) // want `sans-IO package uses a channel: exchange Events and Actions with the loop`
	go func() {}()          // want `sans-IO package uses a go statement: emit an Action and let the loop schedule the work`
	ch <- 1                 // want `sans-IO package sends on a channel: emit an Action`
	<-ch                    // want `sans-IO package receives from a channel: wait for an Event`
	select {                // want `sans-IO package uses a select statement: wait for an Event`
	case v := <-in: // want `sans-IO package receives from a channel: wait for an Event`
		_ = v
	default:
	}
}

type byID map[uint64]string

func iteration(m map[string]int, n byID) int {
	sum := 0
	for _, v := range m { // want `sans-IO package ranges over a map: iteration order is random; iterate sorted keys`
		sum += v
	}
	for k := range n { // want `sans-IO package ranges over a map: iteration order is random; iterate sorted keys`
		sum += int(k)
	}
	return sum
}
