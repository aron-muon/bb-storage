package program

import (
	"context"
	"log"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
)

// runMainErrorLogger is used by RunMain() to capture errors returned by
// goroutines. Each error is logged. Shutdown is initiated as soon as
// the first error arrives.
type runMainErrorLogger struct {
	shutdownStarted sync.Once
	shutdownFunc    func()
	cancel          context.CancelFunc
}

func (el *runMainErrorLogger) Log(err error) {
	log.Print("Fatal error: ", err)
	el.startShutdown(func() {
		os.Exit(1)
	})
}

func (el *runMainErrorLogger) startShutdown(shutdownFunc func()) {
	el.shutdownStarted.Do(func() {
		el.shutdownFunc = shutdownFunc
		el.cancel()
	})
}

// terminateWithSignal terminates the current process after a graceful
// shutdown initiated by `terminationSignal`. The previous implementation
// raised the same signal back to the process via signal.Reset() +
// process.Signal() so the container/init system would observe a
// signal-style exit (e.g. 128+SIGTERM=143). Two issues with that:
//
//  1. signal.Reset() only disables Go's user-channel routing for the
//     signal — it does NOT restore SIG_DFL. The runtime's signal
//     trampoline is still installed and intercepts the raised signal,
//     dispatching it to runtime.dieFromSignal().
//
//  2. runtime.dieFromSignal() (runtime/signal_unix.go) attempts to die
//     from the signal via raise(sig) -> osyield x3 -> setsig(SIG_DFL) +
//     raise(sig) -> osyield x3, then falls through to a hard
//     `exit(2)`. PID 1 in a PID namespace and multi-goroutine programs
//     reliably hit that fall-through, surfacing a spurious exit 2 to
//     k8s/systemd despite a clean shutdown. The earlier
//     `time.Sleep + os.Exit()` fallback in this function never got a
//     chance to run because the runtime exited first.
//
// More background:
//   - https://github.com/golang/go/issues/19326
//   - https://github.com/golang/go/issues/46321
//
// We initiated this shutdown intentionally (caller signal or routines
// finishing cleanly), so just exit 0 directly. terminationSignal is
// retained in the signature for API stability and is intentionally
// unused.
func terminateWithSignal(currentPID int, terminationSignal os.Signal) {
	_ = currentPID
	_ = terminationSignal
	if runtime.GOOS == "windows" {
		os.Exit(1)
	}
	os.Exit(0)
}

var terminationSignals = []os.Signal{
	os.Interrupt,
	syscall.SIGTERM,
}

// RunMain runs a program that supports graceful termination. Programs
// consist of a pool of routines that may have dependencies on each
// other. Programs terminate if one of the following three cases occur:
//
//   - The root routine and all of its siblings have terminated. In that
//     case the program terminates with exit code 0.
//
//   - One of the routines fails with a non-nil error. In that case the
//     program terminates with exit code 1.
//
//   - The program receives SIGINT or SIGTERM. In that case the program
//     will terminate with that signal.
//
// In case termination occurs, all remaining routines are canceled,
// respecting dependencies between these routines. This can for example
// be used to ensure an outgoing database connection is terminated after
// an integrated RPC server is shut down.
func RunMain(routine Routine) {
	currentPID := os.Getpid()
	relaunchIfPID1(currentPID)

	ctx, cancel := context.WithCancel(context.Background())
	errorLogger := &runMainErrorLogger{
		cancel: cancel,
	}

	// Handle incoming signals.
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, terminationSignals...)
	go func() {
		receivedSignal := <-signalChan
		log.Printf("Received %#v signal. Initiating graceful shutdown.", receivedSignal.String())
		errorLogger.startShutdown(func() {
			terminateWithSignal(currentPID, receivedSignal)
		})
	}()

	// Launch the initial routine and any goroutines that it spawns.
	run(ctx, errorLogger, routine)

	// If none of the routines failed and we didn't get signalled,
	// terminate with exit code zero.
	errorLogger.startShutdown(func() {
		os.Exit(0)
	})
	errorLogger.shutdownFunc()
}
