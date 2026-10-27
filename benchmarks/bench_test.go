package benchmarks

import (
	"testing"

	"github.com/ivanjoz/genix-orm/dataframe"
)

// ─────────────────────────────────────────────────────────────────────────────
// One benchmark per phase, a sub-benchmark per case (sales, enrollment, movements):
//
//	LinesOf     record → lines, the whole corpus (Table.LinesOf)
//	Histories   lines + logged changes → History (NewHistoryOfChanges)
//	Rebuild     the engine alone: precomputed Histories → files (sum at the target,
//	            encode, slots, commit)
//	Build       a real rebuild in memory: records → lines → Histories → files, page
//	            by page as a database backend scans them. The number to bring down.
//	Compact     the run at t1 over a partition built at t0: ~1,200 records written
//	            in between
//
// Each reports ns/line (the lines the records put in the frames now), so the cases
// compare whatever their size.
// ─────────────────────────────────────────────────────────────────────────────

func perCase(b *testing.B, run func(b *testing.B, c *Case)) {
	for _, c := range Cases() {
		lineCount := c.LineCount()
		c.Histories()
		b.Run(c.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			run(b, c)
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(lineCount), "ns/line")
		})
	}
}

func BenchmarkLinesOf(b *testing.B) {
	perCase(b, func(b *testing.B, c *Case) {
		for b.Loop() {
			for _, record := range c.Records {
				if _, err := c.Table.LinesOf(record); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
}

func BenchmarkHistories(b *testing.B) {
	perCase(b, func(b *testing.B, c *Case) {
		lines := c.Lines()
		for b.Loop() {
			for i := range c.Records {
				if _, err := c.Table.NewHistoryOfChanges(partition, c.Keys[i], lines[i], c.Changes[i]); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
}

func BenchmarkRebuild(b *testing.B) {
	perCase(b, func(b *testing.B, c *Case) {
		for b.Loop() {
			if _, _, err := c.build(false); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkBuild(b *testing.B) {
	perCase(b, func(b *testing.B, c *Case) {
		for b.Loop() {
			if _, _, err := c.build(true); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkCompact(b *testing.B) {
	perCase(b, func(b *testing.B, c *Case) {
		builtBackend, builtStore, err := c.build(false)
		if err != nil {
			b.Fatal(err)
		}
		b.ResetTimer()
		for b.Loop() {
			b.StopTimer()
			backend, store := newMemoryBackend(c, true), cloneStore(builtStore)
			backend.state = builtBackend.state
			b.StartTimer()
			if err := dataframe.Compact(c.Table, backend, store, partition, t1); err != nil {
				b.Fatal(err)
			}
		}
	})
}
