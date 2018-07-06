package routine_pool

// https://segmentfault.com/a/1190000015464889

import (
	"errors"
	"sync"
	"sync/atomic"
)

var (
	ErrPoolClosed = errors.New("Error: pool closed")
)

type sig struct{}

type Worker struct {
	pool *Pool
	task chan f
}

type f func() error

type Pool struct {
	capacity int32

	numRunning int32

	freeSignal chan sig

	workers []*Worker

	release chan sig

	lock sync.Mutex

	once sync.Once
}

func (p *Pool) Submit(task f) error {
	if len(p.release) > 0 {
		return ErrPoolClosed
	}
	w := p.getWorker()
	w.sendTask(task)
	return nil
}

// getWorker returns an available worker to run the tasks
func (p *Pool) getWorker() *Worker {
	var w *Worker
	waiting := false // indicates whether all the available workers are being used

	p.lock.Lock()
	workers := p.workers
	n := len(workers) - 1

	if n < 0 {
		// all workers are used
		if p.numRunning >= p.capacity {
			waiting = true
		} else {
			p.numRunning++
		}
	} else {
		<-p.freeSignal
		w = workers[n]
		workers[n] = nil
		p.workers = workers[:n]
	}

	p.lock.Unlock()

	if waiting {
		// block until worker is available
		<-p.freeSignal
		p.lock.Lock()
		workers = p.workers
		l := len(workers) - 1
		w = workers[l]
		workers[l] = nil
		p.workers = workers[:l]
		p.lock.Unlock()
	} else if w == nil {
		// no available worker but pool is not full, create a new worker then
		w = &Worker{
			pool: p,
			task: make(chan f),
		}
		w.run()
	}

	return w
}

// putWorker puts a worker back into free pool, recycling the goroutines.
func (p *Pool) putWorker(worker *Worker) {
	p.lock.Lock()
	p.workers = append(p.workers, worker)
	p.lock.Unlock()
	p.freeSignal <- sig{}
}

func (p *Pool) Cap() int {
	return int(p.capacity)
}

// ReSize change the capacity of this pool
func (p *Pool) ReSize(size int) {
	if size < p.Cap() {
		diff := p.Cap() - size
		for i := 0; i < diff; i++ {
			p.getWorker().stop()
		}
	} else if size == p.Cap() {
		return
	}
	atomic.StoreInt32(&p.capacity, int32(size))
}

func (w *Worker) run() {
	//atomic.AddInt32(&w.pool.running, 1)
	go func() {
		//监听任务列表，一旦有任务立马取出运行
		for f := range w.task {
			if f == nil {
				atomic.AddInt32(&w.pool.numRunning, -1)
				return
			}
			f()

			//回收复用
			w.pool.putWorker(w)
		}
	}()
}

// stop this worker.
func (w *Worker) stop() {
	w.sendTask(nil)
}

func (w *Worker) sendTask(task f) {
	w.task <- task
}
