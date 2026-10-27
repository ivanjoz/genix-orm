package benchmarks

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"sync"
	"time"
	"unsafe"

	"github.com/ivanjoz/genix-orm/dataframe"
	"github.com/ivanjoz/genix-orm/utils"
)

// ─────────────────────────────────────────────────────────────────────────────
// The corpus: per shape, RecordCount records of one partition, seeded, so every run
// and every commit sees the same ones. Around them, two moments: T0, when the
// partition is built (Rebuild, its target T0 − TargetLag), and T1, 15 minutes later,
// when it is compacted.
//
//	~4%   changed before T0's target: the rebuild walks past their log entry
//	~1%   changed between T0 and T1 (≈1,000): the rebuild takes their lines from
//	      the log, the compaction moves them
//	~0.2% inserted between T0 and T1: absent from the build, added by the compaction
//
// A changed record's previous version differs the way that shape's records change
// (a sale paid and delivered, an enrollment's minutes, a movement's quantities).
// ─────────────────────────────────────────────────────────────────────────────

// RecordCount is the records of every case.
const RecordCount = 100_000

const partition = 1

var (
	t0 = time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)
	t1 = t0.Add(15 * time.Minute)
	// target0 is what the build at t0 sums the records at.
	target0 = utils.SUnixTimeOf(t0) - dataframe.TargetLag
	target1 = utils.SUnixTimeOf(t1) - dataframe.TargetLag
)

// Case is one shape's corpus: its table, its records as stored now, and what a backend reads of each.
type Case struct {
	Name    string
	Table   *dataframe.Table
	Records []unsafe.Pointer
	// Keys, Changes and LastWrites are per record: its key, its logged changes (newest first, as a
	// backend passes them to NewHistoryOfChanges) and its last write's moment (0: none in the log).
	Keys       []string
	Changes    [][]dataframe.HistoryChange
	LastWrites []int64

	linesOnce sync.Once
	lines     []dataframe.RecordLines
	historiesOnce sync.Once
	histories     []dataframe.History
}

// recordVersions is one generated record: what it holds now and, when it changed in the log's
// window, what it held before (nil: unchanged) and when; or when it was inserted (0: long ago).
type recordVersions struct {
	current, previous unsafe.Pointer
	changeMoment      int64
	insertMoment      int64
}

// eventOf rolls what happens to a record in the log's window.
func eventOf(rng *rand.Rand, index int) (isChanged bool, changeMoment, insertMoment int64) {
	switch roll := rng.IntN(100); {
	case roll < 4:
		return true, target0 - 1 - rng.Int64N(20_000), 0
	case roll == 4:
		return true, target0 + 1 + rng.Int64N(target1-target0-10), 0
	case roll == 5 && index%5 == 0:
		return false, 0, target0 + 1 + rng.Int64N(target1-target0-10)
	}
	return false, 0, 0
}

func newCase(name string, table *dataframe.Table, generate func(rng *rand.Rand, index int) recordVersions) *Case {
	rng := rand.New(rand.NewPCG(uint64(len(name)), 0x5eed))
	c := &Case{Name: name, Table: table, Records: make([]unsafe.Pointer, RecordCount), Keys: make([]string, RecordCount),
		Changes: make([][]dataframe.HistoryChange, RecordCount), LastWrites: make([]int64, RecordCount)}
	for i := range RecordCount {
		versions := generate(rng, i)
		c.Records[i], c.Keys[i] = versions.current, strconv.Itoa(1_000_000+i)
		if versions.previous != nil {
			body := table.EncodeChangedLines(mustLinesOf(table, versions.previous), mustLinesOf(table, versions.current))
			if body != nil {
				c.Changes[i] = append(c.Changes[i], dataframe.HistoryChange{Moment: versions.changeMoment, Body: body})
				c.LastWrites[i] = versions.changeMoment
			}
		}
		if versions.insertMoment > 0 {
			c.Changes[i] = append(c.Changes[i], dataframe.HistoryChange{Moment: versions.insertMoment, IsInsert: true})
			c.LastWrites[i] = max(c.LastWrites[i], versions.insertMoment)
		}
	}
	return c
}

func mustLinesOf(table *dataframe.Table, record unsafe.Pointer) dataframe.RecordLines {
	lines, err := table.LinesOf(record)
	if err != nil {
		panic(err)
	}
	return lines
}

// Lines are every record's lines, computed once.
func (c *Case) Lines() []dataframe.RecordLines {
	c.linesOnce.Do(func() {
		c.lines = make([]dataframe.RecordLines, len(c.Records))
		for i, record := range c.Records {
			c.lines[i] = mustLinesOf(c.Table, record)
		}
	})
	return c.lines
}

// Histories are every record's History, computed once.
func (c *Case) Histories() []dataframe.History {
	c.historiesOnce.Do(func() {
		lines := c.Lines()
		c.histories = make([]dataframe.History, len(c.Records))
		for i := range c.Records {
			history, err := c.Table.NewHistoryOfChanges(partition, c.Keys[i], lines[i], c.Changes[i])
			if err != nil {
				panic(err)
			}
			c.histories[i] = history
		}
	})
	return c.histories
}

// LineCount is the lines every record puts in the frames now.
func (c *Case) LineCount() int {
	count := 0
	for _, recordLines := range c.Lines() {
		for _, frameLines := range recordLines {
			count += len(frameLines)
		}
	}
	return count
}

// ── The cases ──

var (
	casesOnce sync.Once
	cases     []*Case
)

// Cases are the three shapes' corpora, generated once per process.
func Cases() []*Case {
	casesOnce.Do(func() {
		cases = []*Case{
			newCase("sales", compileSales(), generateSale),
			newCase("enrollment", compileEnrollment(), generateEnrollment),
			newCase("movements", compileMovements(), generateMovement),
		}
	})
	return cases
}

// CaseNamed returns the case called name.
func CaseNamed(name string) *Case {
	index := slices.IndexFunc(Cases(), func(c *Case) bool { return c.Name == name })
	if index < 0 {
		panic(fmt.Sprintf("no case %q", name))
	}
	return Cases()[index]
}

// ── sales: 365 days, 2,000 products (a few top sellers), 1–10 lines, genix's statuses ──

const (
	saleFirstDay   = 20454 // 2026-01-01 as a UnixDay
	saleDays       = 365
	saleProducts   = 2000
	saleWarehouses = 4
)

// saleProductPrice is a product's unit price in cents, round like a shelf price; one in ten products
// sells in sixths or twelfths.
func saleProductPrice(productID int32) (price, subPrice int32, subDivisor int16) {
	price = 50 + (productID*37%400)*10
	if productID%10 == 0 {
		subDivisor = int16(6 + 6*(productID/10%2))
		return price, price/int32(subDivisor) + 5, subDivisor
	}
	return price, 0, quantityDivisorNone
}

func generateSale(rng *rand.Rand, index int) recordVersions {
	sale := &SaleOrder{CompanyID: partition, ID: int64(1_000_000 + index), Date: int16(saleFirstDay + rng.IntN(saleDays)),
		WarehouseID: int32(1 + rng.IntN(saleWarehouses)), ClientID: int32(rng.IntN(3000)), CurrencyType: currencyPEN}
	if rng.IntN(10) == 0 {
		sale.CurrencyType, sale.ExchangeRate = currencyUSD, int32(3700+rng.IntN(100))
	}
	switch roll := rng.IntN(100); {
	case roll < 5:
		sale.Status, sale.AnnulReason = OrderStatusAnnulled, "cliente desistió de la compra"
	case roll < 15:
		sale.Status = OrderStatusPending
	case roll < 25:
		sale.Status = OrderStatusPaid
	case roll < 35:
		sale.Status = OrderStatusDelivered
	default:
		sale.Status = OrderStatusCompleted
	}
	lineCount := 1 + rng.IntN(10)
	for range lineCount {
		// Skewed: a few products sell in most sales.
		productID := int32(1 + rng.IntN(1+rng.IntN(saleProducts)))
		price, subPrice, subDivisor := saleProductPrice(productID)
		quantity := int32(1+rng.IntN(5)) * quantityLineScale
		if subDivisor > 1 && rng.IntN(3) == 0 {
			quantity = int32(rng.IntN(3))*quantityLineScale + int32(1+rng.IntN(int(subDivisor)-1))
		}
		sale.DetailProductsIDs = append(sale.DetailProductsIDs, productID)
		sale.DetailPrices = append(sale.DetailPrices, price)
		sale.DetailSubPrices = append(sale.DetailSubPrices, subPrice)
		sale.DetailQuantities = append(sale.DetailQuantities, quantity)
		sale.DetailSubDivisor = append(sale.DetailSubDivisor, subDivisor)
		sale.DetailProductSkus = append(sale.DetailProductSkus, "SKU-"+strconv.Itoa(int(productID)))
		sale.DetailProductLotIDs = append(sale.DetailProductLotIDs, productID*10+int32(rng.IntN(3)))
		sale.DetailProductPresentations = append(sale.DetailProductPresentations, 0)
		sale.TotalAmount += price * (quantity / quantityLineScale)
	}
	sale.TaxAmount = sale.TotalAmount * 18 / 118

	versions := recordVersions{current: unsafe.Pointer(sale)}
	isChanged, changeMoment, insertMoment := eventOf(rng, index)
	versions.insertMoment = insertMoment
	if isChanged {
		previous := *sale
		previous.DetailQuantities = slices.Clone(sale.DetailQuantities)
		if sale.Status == OrderStatusCompleted || sale.Status == OrderStatusAnnulled {
			previous.Status = OrderStatusPending // paid and delivered, or annulled, since
		} else {
			previous.DetailQuantities[0] += quantityLineScale // a line corrected
		}
		versions.previous, versions.changeMoment = unsafe.Pointer(&previous), changeMoment
	}
	return versions
}

// ── enrollment: 5,000 employees in 100 teams of 20 departments, 40 programs, 3 years ──

const (
	enrollmentEmployees = 5000
	enrollmentTeams     = 100
	enrollmentPrograms  = 40
)

func generateEnrollment(rng *rand.Rand, index int) recordVersions {
	employeeID := int32(1 + rng.IntN(enrollmentEmployees))
	teamID := 1 + employeeID%enrollmentTeams
	minutes := int32(30 + 15*rng.IntN(40))
	enrollment := &Enrollment{CompanyID: partition, ID: int64(1_000_000 + index), Year: int16(2024 + rng.IntN(3)),
		ProgramID: int32(1 + rng.IntN(enrollmentPrograms)), DepartmentID: 1 + teamID%20, TeamID: teamID, EmployeeID: employeeID,
		Minutes: minutes, Points: int32(rng.IntN(101)), Hours: float64(minutes) / 60, Status: 1, Comment: "inscripción regular"}
	if rng.IntN(100) < 3 {
		enrollment.Status = 0
	}
	versions := recordVersions{current: unsafe.Pointer(enrollment)}
	isChanged, changeMoment, insertMoment := eventOf(rng, index)
	versions.insertMoment = insertMoment
	if isChanged {
		previous := *enrollment
		previous.Minutes, previous.Hours = enrollment.Minutes/2, enrollment.Hours/2
		if rng.IntN(4) == 0 {
			previous.Status = 1 - enrollment.Status
		}
		versions.previous, versions.changeMoment = unsafe.Pointer(&previous), changeMoment
	}
	return versions
}

// ── movements: 10 warehouses × 104 weeks of files, 3,000 products with up to 4 lots, 1–12 lines ──

const (
	movementWarehouses = 10
	movementWeeks      = 104
	movementProducts   = 3000
)

func generateMovement(rng *rand.Rand, index int) recordVersions {
	movement := &StockMovement{CompanyID: partition, ID: int64(1_000_000 + index), WarehouseID: int32(1 + rng.IntN(movementWarehouses)),
		Week: int16(300 + rng.IntN(movementWeeks)), Kind: int8(1 + rng.IntN(2)), Notes: "guía de remisión"}
	if rng.IntN(50) == 0 {
		movement.Kind = 0
	}
	for range 1 + rng.IntN(12) {
		productID := int32(1 + rng.IntN(1+rng.IntN(movementProducts)))
		movement.ProductIDs = append(movement.ProductIDs, productID)
		movement.LotIDs = append(movement.LotIDs, productID*10+int32(rng.IntN(4)))
		movement.Quantities = append(movement.Quantities, float64(1+rng.IntN(100_000))/1000)
		movement.UnitCosts = append(movement.UnitCosts, float64(100+rng.IntN(50_000))/100)
	}
	versions := recordVersions{current: unsafe.Pointer(movement)}
	isChanged, changeMoment, insertMoment := eventOf(rng, index)
	versions.insertMoment = insertMoment
	if isChanged {
		previous := *movement
		previous.Quantities = slices.Clone(movement.Quantities)
		previous.Quantities[0] *= 2
		if rng.IntN(4) == 0 {
			previous.Kind = 0 // annulled, then restored
		}
		versions.previous, versions.changeMoment = unsafe.Pointer(&previous), changeMoment
	}
	return versions
}
