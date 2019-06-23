package common

import "time"

type Qos struct {
	Bucket chan struct{}

	limit int // qps
	close bool
}

func StartQoS(limit int) *Qos {
	q := new(Qos)
	q.limit = limit
	q.Bucket = make(chan struct{}, limit)

	go q.timer()
	return q
}

func (q *Qos) timer() {
	for range time.NewTicker(1 * time.Second).C {
		if q.close {
			return
		}
		for i := 0; i < q.limit; i++ {
			select {
			case q.Bucket <- struct{}{}:
			default:
				// break if bucket if full
				break
			}
		}
	}
}

func (q *Qos) Close() {
	q.close = true
}
