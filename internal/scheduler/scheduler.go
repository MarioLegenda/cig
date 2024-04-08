package scheduler

import (
	"context"
	"fmt"
	job2 "github.com/MarioLegenda/cig/internal/job"
	"github.com/MarioLegenda/cig/pkg/result"
	"sync/atomic"
	"time"
)

// TODO: create balancer for event scheduler since currently, scheduler is a trowaway instance for every db run,
// TODO: possibility for spawning too many goroutines is not good

type scheduler struct {
	workers []int
	jobs    chan job
	writer  chan result.Result[job2.SearchResult]

	finishedJobs atomic.Int32

	closeCtx  context.Context
	cancelCtx context.CancelFunc

	sentJobs []job
}

type job struct {
	id  int
	fn  job2.JobFn
	ctx context.Context
}

type Scheduler interface {
	Schedule(id int) error
	Start() error
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

func (s *scheduler) Start() error {
	if err := validateJobs(s); err != nil {
		return err
	}

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

	for _, j := range s.sentJobs {
		s.jobs <- j
	}

	return nil
}

func (s *scheduler) Send(id int, fn job2.JobFn, ctx context.Context) {
	s.sentJobs = append(s.sentJobs, job{
		id:  id,
		fn:  fn,
		ctx: ctx,
	})
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

func validateJobs(s *scheduler) error {
	workers := s.workers
	sentJobs := s.sentJobs

	if len(workers) != len(sentJobs) {
		return fmt.Errorf("Internal error. There should be equal number of workers and sent jobs. This is a bug.")
	}

	return nil
}
