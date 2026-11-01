// Command maelstrom runs one replica of a Raft group as a Maelstrom node serving the lin-kv workload: JSON messages on stdin and stdout, peers' Raft messages over Maelstrom's network.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"log"
	"os"
	"time"

	"github.com/nvcnvn/rock-solid/internal/core"
)

func main() {
	stale := flag.Bool("stale-reads", false, "serve reads from the local map without ReadIndex: a planted bug the checker must catch")
	flag.Parse()

	lines := make(chan []byte)
	go func() {
		sc := bufio.NewScanner(os.Stdin)
		sc.Buffer(nil, 16<<20)
		for sc.Scan() {
			lines <- append([]byte(nil), sc.Bytes()...)
		}
		if err := sc.Err(); err != nil {
			log.Fatal(err)
		}
		close(lines)
	}()

	start := time.Now()
	now := func() core.Tick { return core.TickFromDuration(time.Since(start)) }
	n := newNode(*stale)
	w := bufio.NewWriter(os.Stdout)
	enc := json.NewEncoder(w)
	timer := time.NewTimer(time.Hour)
	for {
		select {
		case line, ok := <-lines:
			if !ok {
				return
			}
			var m message
			if err := json.Unmarshal(line, &m); err != nil {
				log.Printf("undecodable message %q: %v", line, err)
				continue
			}
			n.handle(now(), &m)
		case <-timer.C:
			n.tick(now())
		}
		for i := range n.sent {
			if err := enc.Encode(&n.sent[i]); err != nil {
				log.Fatal(err)
			}
		}
		n.sent = n.sent[:0]
		if err := w.Flush(); err != nil {
			log.Fatal(err)
		}
		if n.wake != 0 {
			timer.Reset(time.Until(start.Add(n.wake.Duration())))
		}
	}
}
