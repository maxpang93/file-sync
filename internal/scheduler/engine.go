package scheduler

import (
	"context"
	"log"
	"sync"
	"time"
)

type Engine struct {
	interval time.Duration
	task     func() error
	wg       sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc
}

func NewEngine(interval time.Duration, task func() error) *Engine {
	ctx, cancel := context.WithCancel(context.Background())
	return &Engine{
		interval: interval,
		task:     task,
		ctx:      ctx,
		cancel:   cancel,
	}
}

func (e *Engine) Start() {
	e.wg.Add(1)

	go func() {
		defer e.wg.Done()

		ticker := time.NewTicker(e.interval)
		defer ticker.Stop()

		log.Println("Engine started...")
		for {
			select {
			case <-e.ctx.Done():
				log.Println("Engine loop stopping...")
				return
			case <-ticker.C:
				if err := e.task(); err != nil {
					log.Printf("Scheduled task failed: %v", err)
				}
			}
		}
	}()
}

func (e *Engine) Stop() {
	log.Println("Stopping engine...")
	e.cancel()
	e.wg.Wait()
	log.Println("Engine stopped sucessfully.")
}
