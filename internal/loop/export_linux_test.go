package loop

// TimerFD is the timerfd Arm sets.
func (e *Epoll) TimerFD() int { return e.timer }
