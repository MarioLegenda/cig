package scheduler

import (
	"context"
	"fmt"
	"github.com/MarioLegenda/cig/pkg/result"
	job2 "github.com/MarioLegenda/cig/pkg/syntax/job"
	"time"
)

type JobFn = func(writer chan result.Result[job2.SearchResult])

type scheduler struct {
	workers []int
	jobs    chan job
	writers []chan result.Result[job2.SearchResult]

	closeCtx  context.Context
	cancelCtx context.CancelFunc
}

type job struct {
	id  int
	fn  JobFn
	ctx context.Context
}

type Scheduler interface {
	Schedule(id int) error
	Start()
	Send(id int, fn JobFn, ctx context.Context)
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
	for _, w := range s.workers {
		go func(id int) {
			for {
				job := <-s.jobs
				if job.id != id {
					continue
				}

				select {
				case <-s.closeCtx.Done():
					return
				default:
					fn := job.fn
					writer := make(chan result.Result[job2.SearchResult])
					fn(writer)
					s.writers = append(s.writers, writer)
					return
				}
			}
		}(w)
	}
}

func (s *scheduler) Send(id int, fn JobFn, ctx context.Context) {
	j := job{
		id:  id,
		fn:  fn,
		ctx: ctx,
	}

	s.jobs <- j
}

func (s *scheduler) Results() []result.Result[job2.SearchResult] {
	results := make([]result.Result[job2.SearchResult], len(s.writers))
	for i, w := range s.writers {
		results[i] = <-w
		close(w)
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
		jobs:      nil,
		closeCtx:  ctx,
		cancelCtx: cancel,
	}
}

func newJob(id int, fn JobFn, ctx context.Context) job {
	return job{
		id:  id,
		fn:  fn,
		ctx: ctx,
	}
}
