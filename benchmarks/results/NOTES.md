# Tried and dropped

- **Scatter the Sums into sorted order before summing** (2026-10-27): read the lines in insertion
  order and write each one's Sums at its rank, unpacking the groups from the packed keys, so the
  summing pass reads sequentially. A/B against 5a46606: Rebuild and Build within noise
  (sales 87 → 92 ms, enrollment 16 → 14 ms), bytes +9–27%. The scattered writes cost what the
  gathered reads did. Kept the gather.
