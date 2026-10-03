package utils

import (
	"context"
	"fmt"
	"log"
	"time"
)

func Retry(ctx context.Context, maxRetries int, delay time.Duration, op func() error) error {
	var err error
	for i := 1; i <= maxRetries; i++ {
		err = op()
		if err == nil {
			return nil
		}
		log.Printf("Attempt %d failed: %v\n", i, err)
		if i < maxRetries {
			// Use select with ctx.Done instead of time.Sleep so the
			// wait period can be interrupted immediately if the context is cancelled.
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return fmt.Errorf("failed after %d attempts; last error: %w", maxRetries, err)
}
