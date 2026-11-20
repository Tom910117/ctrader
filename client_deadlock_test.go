package ctrader

import (
	"errors"
	"testing"
	"time"

	"golang.org/x/exp/slog"

	"github.com/diegobernardes/ctrader/openapi"
)

// deadlockFakeTransport is a minimal stand-in for clientTransport.
// It opens no real network connection — it exists only to satisfy the
// interface so that Client.Stop() can call transport.stop() without a
// nil-pointer panic. send() always fails, simulating a dropped connection.
type deadlockFakeTransport struct{}

func (deadlockFakeTransport) start(string) error                   { return nil }
func (deadlockFakeTransport) stop() error                          { return nil }
func (deadlockFakeTransport) send([]byte) error                    { return errors.New("simulated network failure") }
func (deadlockFakeTransport) setHandler(func([]byte), func(error)) {}

// TestHandlerError_SelfWaitDeadlock reproduces a deadlock in the current
// handlerError/Stop implementation, with no real network connection
// required.
//
// Real-world trigger: the TCP connection to cTrader drops (network blip,
// server restart, idle disconnect, etc). The next heartbeat tick inside
// keepalive() fails to send and calls, from inside itself:
//
//	c.handlerError(fmt.Errorf("failed to send the heartbeat event: %w", err))
//
// handlerError() then calls c.Stop(), whose first statement is:
//
//	c.wg.Wait()
//
// c.wg is the *same* WaitGroup that the currently-running keepalive
// goroutine holds (incremented once via c.wg.Add(1) at the top of
// keepalive(), and only decremented by that same goroutine's deferred
// c.wg.Done() when it returns). Because handlerError() was invoked FROM
// INSIDE that goroutine and has not returned yet, Done() is never reached.
// The goroutine ends up waiting for itself to finish before it can finish.
//
// Effect on a running server: the HTTP server keeps accepting webhook
// requests, but every request that needs a cTrader round-trip blocks
// forever (or until its own context deadline, if one was set) because
// nothing is reading the TCP connection anymore. There is no crash and no
// error log after the initial "Asynchronous error" line — the process
// looks alive but is permanently unable to talk to cTrader again.
//
// This test simulates the exact call chain above, with a 10-second internal
// timeout so it fails fast even without any special flags. For the full
// proof (every goroutine's stack trace, showing the goroutine parked in
// sync.(*WaitGroup).Wait inside Client.Stop, called from
// Client.handlerError), run:
//
//	go test -run TestHandlerError_SelfWaitDeadlock -timeout 5s ./...
//
// and the test binary itself will dump all goroutine stacks on timeout.
func TestHandlerError_SelfWaitDeadlock(t *testing.T) {
	c := &Client{
		Logger:          slog.Default(),
		transport:       deadlockFakeTransport{},
		requestRegistry: make(map[string]chan *openapi.ProtoMessage),
	}

	// Mirrors c.wg.Add(1) at the top of keepalive(): "a keepalive goroutine
	// is currently running and has not yet returned".
	c.wg.Add(1)

	done := make(chan struct{})
	go func() {
		defer close(done)
		// This is the exact statement keepalive() executes on a failed
		// send, run here on this goroutine to stand in for "keepalive
		// calling handlerError from inside itself":
		//
		//   c.handlerError(fmt.Errorf("failed to send the heartbeat event: %w", err))
		c.handlerError(errors.New("simulated network failure"))
		// Reaching this line would mean handlerError returned normally —
		// i.e. no deadlock occurred.
		c.wg.Done()
	}()

	select {
	case <-done:
		t.Fatal("handlerError() returned — no deadlock occurred. " +
			"If you are running this against a fixed version of client.go, this is expected.")
	case <-time.After(10 * time.Second):
		t.Fatal("handlerError() is still blocked after 10s. This is the deadlock: " +
			"keepalive() -> handlerError() -> Stop() -> c.wg.Wait() is waiting on the very " +
			"goroutine that is executing this call chain. Re-run with " +
			"`go test -run TestHandlerError_SelfWaitDeadlock -timeout 5s ./...` " +
			"to capture the full goroutine dump as proof.")
	}
}
