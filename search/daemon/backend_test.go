package daemon

import (
	"context"
	"testing"

	"github.com/ivanjoz/genix-orm/db"
)

// TestBackendApply: one bucket per partition, pushes before pops, and a put whose text
// normalizes to nothing is popped.
func TestBackendApply(t *testing.T) {
	fake, client := newFakeServer(t)
	defer client.Close()
	defer fake.conn.Close()

	fake.queueReply("PONG")     // pool checkout
	fake.queueReply("RESULT 1") // PUSHI
	fake.queueReply("RESULT 0") // POPI 5
	fake.queueReply("RESULT 1") // POPI 9

	stop := make(chan struct{})
	defer close(stop)
	var got []string
	done := make(chan struct{})
	go func() {
		got = fake.run(stop)
		close(done)
	}()

	restore := installFakeIngestPool(makePooledConn(client, 20000))
	defer restore()

	puts := []db.TextDoc{{ID: 1, Text: "Piña Golden"}, {ID: 9, Text: " ¿? "}}
	if err := NewBackend().Apply(context.Background(), "products", "p7", puts, []int32{5}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	_ = client.Close()
	<-done

	want := []string{"PING", `PUSHI products p7 1 "pina golden"`, "POPI products p7 5", "POPI products p7 9"}
	if len(got) < len(want) {
		t.Fatalf("got %q, want %q", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("frame %d = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestBackendRejectsBadNames(t *testing.T) {
	if err := NewBackend().Apply(context.Background(), "products", "p-7", []db.TextDoc{{ID: 1, Text: "x"}}, nil); err == nil {
		t.Error("a partition name the daemon cannot take was accepted")
	}

	if hits, err := NewBackend().Search(context.Background(), "products", "p7", "  ¿?  ", 10, 0); err != nil || hits != nil {
		t.Errorf("an empty normalized query: %v %v", hits, err)
	}
}

func TestWeightToScore(t *testing.T) {
	if weightToScore(1.2345) != 1234 || weightToScore(-1) != 0 || weightToScore(0.5) <= weightToScore(0.4) {
		t.Error("scores must keep the order of the weights")
	}
}
