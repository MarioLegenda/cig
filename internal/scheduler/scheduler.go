package scheduler

import (
	"context"
	"fmt"
	job2 "github.com/MarioLegenda/cig/internal/job"
	"github.com/MarioLegenda/cig/pkg/result"
	"sync/atomic"
	"time"
)

type scheduler struct {
	workers []int
	jobs    chan job
	writer  chan result.Result[job2.SearchResult]

	finishedJobs atomic.Int32

	closeCtx  context.Context
	cancelCtx context.CancelFunc
}

type job struct {
	id  int
	fn  job2.JobFn
	ctx context.Context
}

type Scheduler interface {
	Schedule(id int) error
	Start()
	Send(id int, fn job2.JobFn, ctx context.Context)
	Close()
	Results() []result.Result[job2.SearchResult]
}

func (s *scheduler) Schedule(id int) error {
	for _, t := range s.workers {
		if t == id {
			return fmt.Errorf("Worker with id %d already scheduled", id)
		}
	}

	s.workers = append(s.workers, id)

	return nil
}

func (s *scheduler) Start() {
	go func() {
		for {
			if s.finishedJobs.Load() == int32(len(s.workers)) {
				close(s.writer)
				return
			}
		}
	}()

	for _, w := range s.workers {
		go func(id int) {
			for {
				j := <-s.jobs
				if j.id != id {
					continue
				}

				select {
				case <-s.closeCtx.Done():
					return
				default:
					fn := j.fn
					fn(j.id, s.writer, j.ctx)
					s.finishedJobs.Add(1)
					return
				}
			}
		}(w)
	}
}

func (s *scheduler) Send(id int, fn job2.JobFn, ctx context.Context) {
	j := job{
		id:  id,
		fn:  fn,
		ctx: ctx,
	}

	s.jobs <- j
}

func (s *scheduler) Results() []result.Result[job2.SearchResult] {
	results := make([]result.Result[job2.SearchResult], 0)
	for res := range s.writer {
		results = append(results, res)
	}

	return results
}

func (s *scheduler) Close() {
	s.cancelCtx()
}

func New() Scheduler {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	return &scheduler{
		workers:   make([]int, 0),
		jobs:      make(chan job),
		writer:    make(chan result.Result[job2.SearchResult]),
		closeCtx:  ctx,
		cancelCtx: cancel,
	}
}
