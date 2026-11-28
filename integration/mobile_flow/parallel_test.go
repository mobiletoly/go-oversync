package main

import (
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestNormalizeSimulationConcurrency(t *testing.T) {
	tests := []struct {
		name      string
		users     int
		requested int
		want      int
		wantError string
	}{
		{name: "omitted uses total users", users: 500, requested: 0, want: 500},
		{name: "explicit bound", users: 500, requested: 30, want: 30},
		{name: "single user", users: 1, requested: 1, want: 1},
		{name: "negative", users: 10, requested: -1, wantError: "non-negative"},
		{name: "above total", users: 10, requested: 11, wantError: "must not exceed"},
		{name: "invalid total", users: 0, requested: 0, wantError: "total users"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := normalizeSimulationConcurrency(test.users, test.requested)
			if test.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantError) {
					t.Fatalf("error = %v, want containing %q", err, test.wantError)
				}
				return
			}
			if err != nil {
				t.Fatalf("normalize concurrency: %v", err)
			}
			if got != test.want {
				t.Fatalf("concurrency = %d, want %d", got, test.want)
			}
		})
	}
}

func TestRunBoundedUserWorkers_LimitsActiveUsers(t *testing.T) {
	const (
		totalUsers  = 20
		concurrency = 3
	)

	var active atomic.Int64
	var highWater atomic.Int64
	var completed atomic.Int64
	reached := make(chan struct{}, totalUsers)
	release := make(chan struct{})
	done := make(chan struct{})

	go func() {
		runBoundedUserWorkers(totalUsers, concurrency, func(int) {
			current := active.Add(1)
			for {
				observed := highWater.Load()
				if current <= observed || highWater.CompareAndSwap(observed, current) {
					break
				}
			}
			reached <- struct{}{}
			<-release
			active.Add(-1)
			completed.Add(1)
		})
		close(done)
	}()

	for range concurrency {
		select {
		case <-reached:
		case <-time.After(time.Second):
			t.Fatal("workers did not reach the concurrency bound")
		}
	}
	if got := active.Load(); got != concurrency {
		t.Fatalf("active users = %d, want %d", got, concurrency)
	}
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("workers did not complete")
	}

	if got := completed.Load(); got != totalUsers {
		t.Fatalf("completed users = %d, want %d", got, totalUsers)
	}
	if got := highWater.Load(); got != concurrency {
		t.Fatalf("active-user high water = %d, want %d", got, concurrency)
	}
	if got := active.Load(); got != 0 {
		t.Fatalf("active users after completion = %d, want 0", got)
	}
}
