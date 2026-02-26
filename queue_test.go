// queue_test.go
package dque_test

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/joncrlsn/dque"
)

// item2 is the thing we'll be storing in the queue
type item2 struct {
	Id int
}

// item2Builder creates a new item and returns a pointer to it.
// This is used when we load a segment of the queue from disk.
func item2Builder() interface{} {
	return &item2{}
}

// Adds 1 and removes 1 in a loop to ensure that when we've filled
// up the first segment that we delete it and move on to the next segment
func TestQueue_AddRemoveLoop(t *testing.T) {
	testQueue_AddRemoveLoop(t, true /* true=turbo */)
	testQueue_AddRemoveLoop(t, false /* true=turbo */)
}

func testQueue_AddRemoveLoop(t *testing.T, turbo bool) {
	qName := "test1"
	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error removing queue directory", err)
	}

	q := newQ(t, qName, turbo)

	for i := 0; i < 4; i++ {
		if err := q.Enqueue(&item2{i}); err != nil {
			t.Fatal("Error enqueueing", err)
		}
		_, err := q.Dequeue()
		if err != nil {
			t.Fatal("Error dequeueing", err)
		}
	}

	assert(t, 0 == q.Size(), "Size is not 0")

	firstSegNum, lastSegNum := q.SegmentNumbers()
	assert(t, firstSegNum == lastSegNum, "The first segment must match the last")
	assert(t, 2 == firstSegNum, "The first segment is not 2")

	q.Close()
	q = openQ(t, qName, turbo)

	firstSegNum, lastSegNum = q.SegmentNumbers()
	assert(t, firstSegNum == lastSegNum, "After opening, the first segment must match the second")
	assert(t, 2 == firstSegNum, "After opening, the first segment is not 2")

	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error cleaning up the queue directory", err)
	}
}

// Adds 2 and removes 1 in a loop
func TestQueue_Add2Remove1(t *testing.T) {
	testQueue_Add2Remove1(t, true /* true=turbo */)
	testQueue_Add2Remove1(t, false /* true=turbo */)
}

func testQueue_Add2Remove1(t *testing.T, turbo bool) {
	qName := "test1"
	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error removing queue directory", err)
	}

	q := newQ(t, qName, turbo)

	for i := 0; i < 4; i = i + 2 {
		if err := q.Enqueue(&item2{i}); err != nil {
			t.Fatal("Error enqueueing", err)
		}
		if err := q.Enqueue(&item2{i + 1}); err != nil {
			t.Fatal("Error enqueueing", err)
		}
		item, err := q.Dequeue()
		if err != nil {
			t.Fatal("Error dequeueing", err)
		}
		assert(t, item != nil, "Item is nil")
	}

	firstSegNum, lastSegNum := q.SegmentNumbers()
	assert(t, firstSegNum < lastSegNum, "The first segment cannot match the second")
	assert(t, 2 == lastSegNum, "The last segment must be 2")

	q.Close()
	q = openQ(t, qName, turbo)

	firstSegNum, lastSegNum = q.SegmentNumbers()
	assert(t, firstSegNum < lastSegNum, "After opening, the first segment can not match the second")
	assert(t, 2 == lastSegNum, "After opening, the last segment must be 2")

	assert(t, 2 == q.Size(), "Queue size is not 2 before peeking")
	obj, err := q.Peek()
	if err != nil {
		t.Fatal("Error peeking at the queue", err)
	}
	assert(t, 2 == q.Size(), "After peeking, queue size must still be 2")
	assert(t, obj != nil, "Peeked object must not be nil.")

	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error cleaning up the queue directory", err)
	}
}

// Adds 9 and removes 8
func TestQueue_Add9Remove8(t *testing.T) {
	testQueue_Add9Remove8(t, true /* true = turbo */)
	testQueue_Add9Remove8(t, false /* true = turbo */)
}

func testQueue_Add9Remove8(t *testing.T, turbo bool) {
	qName := "test1"
	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error removing queue directory", err)
	}

	q := newQ(t, qName, turbo)

	for i := 0; i < 9; i++ {
		if err := q.Enqueue(&item2{i}); err != nil {
			t.Fatal("Error enqueueing", err)
		}
	}

	assert(t, 9 == q.Size(), "the size is calculated wrong.  Should be 9")

	firstSegNum, lastSegNum := q.SegmentNumbers()
	assert(t, 1 == firstSegNum, "the first segment is not 1")
	assert(t, 3 == lastSegNum, "the last segment is not 3")

	for i := 0; i < 8; i++ {
		iface, err := q.Dequeue()
		if err != nil {
			t.Fatal("Error dequeueing:", err)
		}
		assert(t, 8-i == q.Size(), "the size is calculated wrong.")
		item, ok := iface.(item2)
		if ok {
			fmt.Printf("Dequeued %T %t %#v\n", item, ok, item)
			assert(t, i == item.Id, "Unexpected itemId")
		} else {
			item, ok := iface.(*item2)
			assert(t, ok, "Dequeued object is not of type *item2")
			assert(t, i == item.Id, "Unexpected itemId")
		}
	}

	firstSegNum, lastSegNum = q.SegmentNumbers()
	assert(t, firstSegNum == lastSegNum, "The first segment must match the second")
	assert(t, 3 == firstSegNum, "The last segment is not 3")

	q.Close()
	_ = openQ(t, qName, turbo)

	assert(t, firstSegNum == lastSegNum, "After opening, the first segment must match the second")
	assert(t, 3 == lastSegNum, "After opening, the last segment is not 3")

	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error removing queue directory for Add9Remove8:", err)
	}
}

func TestQueue_EmptyDequeue(t *testing.T) {
	testQueue_EmptyDequeue(t, true /* true=turbo */)
	testQueue_EmptyDequeue(t, false /* true=turbo */)
}

func testQueue_EmptyDequeue(t *testing.T, turbo bool) {
	qName := "testEmptyDequeue"
	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error removing queue directory:", err)
	}

	q := newQ(t, qName, turbo)
	assert(t, 0 == q.Size(), "Expected an empty queue")

	item, err := q.Dequeue()
	assert(t, dque.ErrEmpty == err, "Expected an ErrEmpty error")
	assert(t, item == nil, "Expected nil because queue is empty")

	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error cleaning up the queue directory:", err)
	}
}

func TestQueue_NewOrOpen(t *testing.T) {
	testQueue_NewOrOpen(t, true /* true=turbo */)
	testQueue_NewOrOpen(t, false /* true=turbo */)
}

func testQueue_NewOrOpen(t *testing.T, turbo bool) {
	qName := "testNewOrOpen"
	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error removing queue directory:", err)
	}

	q := newOrOpenQ(t, qName, turbo)
	q.Close()

	q = newOrOpenQ(t, qName, turbo)
	q.Close()

	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error cleaning up the queue directory:", err)
	}
}

func TestQueue_Turbo(t *testing.T) {
	qName := "testNewOrOpen"
	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error removing queue directory:", err)
	}

	q := newQ(t, qName, false)

	if err := q.TurboOff(); err == nil {
		t.Fatal("Expected an error")
	}
	if err := q.TurboSync(); err == nil {
		t.Fatal("Expected an error")
	}
	if err := q.TurboOn(); err != nil {
		t.Fatal("Error turning on turbo:", err)
	}
	if err := q.TurboOn(); err == nil {
		t.Fatal("Expected an error")
	}
	if err := q.TurboSync(); err != nil {
		t.Fatal("Error running TurboSync:", err)
	}

	start := time.Now()
	for i := 0; i < 1000; i++ {
		if err := q.Enqueue(&item2{i}); err != nil {
			t.Fatal("Error enqueueing:", err)
		}
	}
	elapsedTurbo := time.Since(start)

	assert(t, q.Turbo(), "Expected turbo to be on")
	if err := q.TurboOff(); err != nil {
		t.Fatal("Error turning off turbo:", err)
	}

	start = time.Now()
	for i := 0; i < 1000; i++ {
		if err := q.Enqueue(&item2{i}); err != nil {
			t.Fatal("Error enqueueing:", err)
		}
	}
	elapsedSafe := time.Since(start)

	assert(t, elapsedTurbo < elapsedSafe/2, "Turbo time (%v) must be faster than safe mode (%v)", elapsedTurbo, elapsedSafe)

	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error cleaning up the queue directory:", err)
	}
}

func TestQueue_NewFlock(t *testing.T) {
	qName := "testFlock"
	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error cleaning up the queue directory:", err)
	}

	q, err := dque.New(qName, ".", 3, item2Builder)
	if err != nil {
		t.Fatal("Error creating dque:", err)
	}
	if err = q.Close(); err != nil {
		t.Fatal("Error closing dque:", err)
	}

	// Double-open should fail
	q, err = dque.Open(qName, ".", 3, item2Builder)
	if err != nil {
		t.Fatal("Error opening dque:", err)
	}
	_, err = dque.Open(qName, ".", 3, item2Builder)
	if err == nil {
		t.Fatal("No error during double-open dque")
	}
	if err = q.Close(); err != nil {
		t.Fatal("Error closing dque:", err)
	}

	// Double-close should fail
	q, err = dque.Open(qName, ".", 3, item2Builder)
	if err != nil {
		t.Fatal("Error opening dque:", err)
	}
	if err = q.Close(); err != nil {
		t.Fatal("Error closing dque:", err)
	}
	if err = q.Close(); err == nil {
		t.Fatal("No error during double-closing dque")
	}

	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error removing queue directory:", err)
	}
}

func TestQueue_UseAfterClose(t *testing.T) {
	qName := "testUseAfterClose"
	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error cleaning up the queue directory:", err)
	}

	q, err := dque.New(qName, ".", 3, item2Builder)
	if err != nil {
		t.Fatal("Error creating dque:", err)
	}
	if err = q.Enqueue(&item2{0}); err != nil {
		t.Fatal("Error enqueing item:", err)
	}
	if err = q.Close(); err != nil {
		t.Fatal("Error closing dque:", err)
	}

	const queueClosedError = "queue is closed"

	err = q.Close()
	assert(t, err.Error() == queueClosedError, "Expected error not found: %v", err)

	err = q.Enqueue(&item2{0})
	assert(t, err.Error() == queueClosedError, "Expected error not found: %v", err)

	_, err = q.Dequeue()
	assert(t, err.Error() == queueClosedError, "Expected error not found: %v", err)

	_, err = q.Peek()
	assert(t, err.Error() == queueClosedError, "Expected error not found: %v", err)

	s := q.Size()
	assert(t, s == 0, "Expected 0 size")
	s = q.SizeUnsafe()
	assert(t, s == 0, "Expected 0 size")

	err = q.TurboOn()
	assert(t, err.Error() == queueClosedError, "Expected error not found: %v", err)
	err = q.TurboOff()
	assert(t, err.Error() == queueClosedError, "Expected error not found: %v", err)
	err = q.TurboSync()
	assert(t, err.Error() == queueClosedError, "Expected error not found: %v", err)

	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error removing queue directory:", err)
	}
}

func TestQueue_BlockingBehaviour(t *testing.T) {
	qName := "testBlocking"
	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error removing queue directory:", err)
	}

	q := newQ(t, qName, false)

	go func() {
		assert(t, q.Enqueue(&item2{0}) == nil, "Expected no error")
	}()

	x, err := q.PeekBlock()
	assert(t, err == nil, "Expected no error")
	assert(t, x != nil, "Item is nil")

	x, err = q.DequeueBlock()
	assert(t, err == nil, "Expected no error")
	assert(t, x != nil, "Item is nil")

	_, err = q.Dequeue()
	assert(t, err == dque.ErrEmpty, "Expected ErrEmpty error")

	timeout := time.After(3 * time.Second)
	done := make(chan bool)
	go func() {
		x, err = q.DequeueBlock()
		assert(t, err == nil, "Expected no error")
		assert(t, x != nil, "Item is nil")
		done <- true
	}()
	go func() {
		time.Sleep(1 * time.Second)
		assert(t, q.Enqueue(&item2{2}) == nil, "Expected no error")
	}()

	select {
	case <-timeout:
		t.Fatal("Test didn't finish in time")
	case <-done:
	}

	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error removing queue directory:", err)
	}
}

func TestQueue_BlockingWithClose(t *testing.T) {
	qName := "testBlockingWithClose"
	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error removing queue directory:", err)
	}

	q := newQ(t, qName, false)

	go func() {
		time.Sleep(1 * time.Second)
		assert(t, q.Close() == nil, "Expected no error")
	}()

	timeout := time.After(3 * time.Second)
	done := make(chan bool)
	go func() {
		_, err := q.DequeueBlock()
		assert(t, err == dque.ErrQueueClosed, "Expected ErrQueueClosed error")
		done <- true
	}()

	select {
	case <-timeout:
		t.Fatal("Test didn't finish in time")
	case <-done:
	}

	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error removing queue directory:", err)
	}
}

func TestQueue_BlockingAggressive(t *testing.T) {

	qName := "testBlockingAggressive"
	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error removing queue directory:", err)
	}

	q := newQ(t, qName, false)

	const numProducers = 5
	const numItemsPerProducer = 50
	const numConsumers = 25

	done := make(chan bool)
	var wg sync.WaitGroup
	wg.Add(numProducers * numItemsPerProducer)

	go func() {
		wg.Wait()
		q.Close()
		done <- true
	}()

	for p := 0; p < numProducers; p++ {
		go func(producer int) {
			rng := rand.New(rand.NewSource(int64(producer)))
			for i := 0; i < numItemsPerProducer; i++ {
				s := rng.Intn(150)
				time.Sleep(time.Duration(s) * time.Millisecond)
				assert(t, q.Enqueue(&item2{i}) == nil, "Expected no error")
				fmt.Println("Enqueued item", i, "by producer", producer, "after sleeping", s)
			}
		}(p)
	}

	for c := 0; c < numConsumers; c++ {
		go func(consumer int) {
			for {
				x, err := q.DequeueBlock()
				if err == dque.ErrQueueClosed {
					return
				}
				assert(t, err == nil, "Expected no error")
				fmt.Println("Dequeued item", x, "by consumer", consumer)
				wg.Done()
			}
		}(c)
	}

	timeout := time.After(10 * time.Second)
	select {
	case <-timeout:
		t.Fatal("Test didn't finish in time")
	case <-done:
	}

	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error removing queue directory:", err)
	}
}

// TestQueue_WithCodecRoundtrip verifies that NewOrOpenWithCodec preserves data across close/reopen.
func TestQueue_WithCodecRoundtrip(t *testing.T) {
	qName := "testWithCodecRoundtrip"
	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error removing queue directory:", err)
	}
	defer os.RemoveAll(qName)

	const numItems = 15

	q, err := dque.NewOrOpenWithCodec(qName, ".", 5, item2Builder, dque.GobCodec{})
	if err != nil {
		t.Fatal("Error creating queue:", err)
	}
	for i := 0; i < numItems; i++ {
		if enqErr := q.Enqueue(&item2{i}); enqErr != nil {
			t.Fatalf("Enqueue[%d] failed: %s", i, enqErr)
		}
	}
	assert(t, q.Size() == numItems, "Size mismatch after enqueue: got %d want %d", q.Size(), numItems)
	q.Close()

	// Reopen with same codec and verify all items come back in order.
	q2, err := dque.NewOrOpenWithCodec(qName, ".", 5, item2Builder, dque.GobCodec{})
	if err != nil {
		t.Fatal("Error reopening queue:", err)
	}
	for i := 0; i < numItems; i++ {
		raw, deqErr := q2.Dequeue()
		if deqErr != nil {
			t.Fatalf("Dequeue[%d] failed: %s", i, deqErr)
		}
		got, ok := raw.(*item2)
		assert(t, ok, "Dequeued object is not *item2 at index %d", i)
		assert(t, got.Id == i, "item[%d] Id mismatch: got %d want %d", i, got.Id, i)
	}
	q2.Close()
}

func newOrOpenQ(t *testing.T, qName string, turbo bool) *dque.DQue {
	q, err := dque.NewOrOpen(qName, ".", 3, item2Builder)
	if err != nil {
		t.Fatal("Error creating or opening dque:", err)
	}
	if turbo {
		_ = q.TurboOn()
	}
	return q
}

func newQ(t *testing.T, qName string, turbo bool) *dque.DQue {
	q, err := dque.New(qName, ".", 3, item2Builder)
	if err != nil {
		t.Fatal("Error creating new dque:", err)
	}
	if turbo {
		_ = q.TurboOn()
	}
	return q
}

func openQ(t *testing.T, qName string, turbo bool) *dque.DQue {
	q, err := dque.Open(qName, ".", 3, item2Builder)
	if err != nil {
		t.Fatal("Error opening dque:", err)
	}
	if turbo {
		_ = q.TurboOn()
	}
	return q
}

// assert fails the test if the condition is false.
func assert(tb testing.TB, condition bool, msg string, v ...interface{}) {
	if !condition {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d: "+msg+"\033[39m\n\n", append([]interface{}{filepath.Base(file), line}, v...)...)
		tb.FailNow()
	}
}
