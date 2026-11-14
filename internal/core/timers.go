package core

// Timers is a loop's timer heap: each TimerID armed at most once, the earliest deadline first, ties by ID.
type Timers struct {
	heap []timer
	at   map[TimerID]int
}

type timer struct {
	at Tick
	id TimerID
}

// Set arms id at at, replacing its earlier deadline, as a SetTimer action does.
func (t *Timers) Set(id TimerID, at Tick) {
	if t.at == nil {
		t.at = make(map[TimerID]int)
	}
	if i, ok := t.at[id]; ok {
		t.heap[i].at = at
		t.fix(i)
		return
	}
	t.heap = append(t.heap, timer{at: at, id: id})
	t.at[id] = len(t.heap) - 1
	t.up(len(t.heap) - 1)
}

// Earliest is the earliest deadline armed, false when none is.
func (t *Timers) Earliest() (Tick, bool) {
	if len(t.heap) == 0 {
		return 0, false
	}
	return t.heap[0].at, true
}

// Armed reports whether id is armed.
func (t *Timers) Armed(id TimerID) bool {
	_, ok := t.at[id]
	return ok
}

// Len is the number of timers armed.
func (t *Timers) Len() int { return len(t.heap) }

// PopDue removes and returns the earliest timer due by now.
func (t *Timers) PopDue(now Tick) (TimerID, bool) {
	if len(t.heap) == 0 || t.heap[0].at > now {
		return 0, false
	}
	id := t.heap[0].id
	last := len(t.heap) - 1
	t.swap(0, last)
	t.heap = t.heap[:last]
	delete(t.at, id)
	if last > 0 {
		t.down(0)
	}
	return id, true
}

func (t *Timers) less(i, j int) bool {
	a, b := t.heap[i], t.heap[j]
	return a.at < b.at || a.at == b.at && a.id < b.id
}

func (t *Timers) swap(i, j int) {
	t.heap[i], t.heap[j] = t.heap[j], t.heap[i]
	t.at[t.heap[i].id] = i
	t.at[t.heap[j].id] = j
}

func (t *Timers) fix(i int) {
	if !t.down(i) {
		t.up(i)
	}
}

func (t *Timers) up(i int) {
	for i > 0 {
		p := (i - 1) / 2
		if !t.less(i, p) {
			return
		}
		t.swap(i, p)
		i = p
	}
}

func (t *Timers) down(i int) bool {
	start := i
	for {
		c := 2*i + 1
		if c >= len(t.heap) {
			break
		}
		if r := c + 1; r < len(t.heap) && t.less(r, c) {
			c = r
		}
		if !t.less(c, i) {
			break
		}
		t.swap(i, c)
		i = c
	}
	return i > start
}
