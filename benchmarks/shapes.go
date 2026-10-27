// Package benchmarks measures building DataFrame partitions in memory (records → lines → Histories →
// aggregated, encoded files) through dataframe's public API alone. It lives only on the benchmarks
// branch: main never carries it, its corpus or its dependencies. See README.md.
package benchmarks

import (
	"math"
	"unsafe"

	"github.com/ivanjoz/genix-orm/dataframe"
)

// ─────────────────────────────────────────────────────────────────────────────
// The three shapes under test, each a record type and its frames, declared the way
// their users declare them:
//
//	sales        genix's SaleOrder and its "sale-summary" frame (ToFrames, verbatim):
//	             1 Key (the day), 2 Rows (product, sub-divisor), 6 int32 sums, the
//	             zero sums mask; 1–10 lines per record.
//	enrollment   berryapps' shape: a frame declared by columns, as scylla builds its
//	             LinesOf (one line per record, none when soft-deleted): 2 Keys (year,
//	             program), 3 nested Rows (department, team, employee), 2 int sums and
//	             a float, the record count.
//	movements    a stock ledger: a ToFrames frame with 2 Keys (warehouse, week), 2 Rows
//	             (product, lot), float64 and int64 sums, the record count and the mask;
//	             1–30 lines per record, many files.
// ─────────────────────────────────────────────────────────────────────────────

// ── sales: genix's SaleOrder (backend/sales/types/sales.go) ──

const (
	OrderStatusAnnulled  = int8(0)
	OrderStatusPending   = int8(1)
	OrderStatusPaid      = int8(2)
	OrderStatusDelivered = int8(3)
	OrderStatusCompleted = int8(4)

	currencyPEN         = int8(1)
	currencyUSD         = int8(2)
	exchangeRateScale   = 1000
	quantityLineScale   = int32(1000)
	quantityDivisorNone = int16(1)
)

// SaleOrder holds the fields of genix's SaleOrder that its frame and a stored row carry (the strings
// and slices it doesn't read are kept, so a record weighs what it weighs there).
type SaleOrder struct {
	CompanyID   int32
	Date        int16
	WarehouseID int32
	ID          int64

	DetailProductsIDs          []int32
	DetailPrices               []int32
	DetailSubPrices            []int32
	DetailQuantities           []int32
	DetailSubDivisor           []int16
	DetailProductSkus          []string
	DetailProductLotIDs        []int32
	DetailProductPresentations []int16

	TotalAmount   int32
	TaxAmount     int32
	DebtAmount    int32
	CurrencyType  int8
	ExchangeRate  int32
	ClientID      int32
	Created       int32
	Updated       int32
	UpdateCounter int32
	Status        int8
	AnnulReason   string
}

// SaleSummaryFrame is genix's sale-summary line, verbatim.
type SaleSummaryFrame struct {
	Date          int16 `frame:"key"`
	ProductID     int32 `frame:"row"`
	SubDivisor    int16 `frame:"row"`
	AmountPEN     int32 `frame:"sum"`
	DebtAmountPEN int32 `frame:"sum"`
	SoldUnits     int32 `frame:"sum"`
	SoldSub       int32 `frame:"sum"`
	PendingUnits  int32 `frame:"sum"`
	PendingSub    int32 `frame:"sum"`
}

func getIndex[T int16 | int32](values []T, index int) T {
	if index < 0 || index >= len(values) {
		return 0
	}
	return values[index]
}

func saturateInt32(value int64) int32 {
	return int32(max(min(value, math.MaxInt32), math.MinInt32))
}

// ToSaleSummaryFrames is genix's, with core's and finance's helpers inlined.
func (e *SaleOrder) ToSaleSummaryFrames() []SaleSummaryFrame {
	if e.Status == OrderStatusAnnulled {
		return nil
	}
	isDeliveryPending := e.Status == OrderStatusPending || e.Status == OrderStatusPaid
	isPaymentPending := e.Status == OrderStatusPending || e.Status == OrderStatusDelivered
	lines := make([]SaleSummaryFrame, 0, len(e.DetailProductsIDs))
	for lineIndex, productID := range e.DetailProductsIDs {
		packedQuantity := getIndex(e.DetailQuantities, lineIndex)
		if productID <= 0 || packedQuantity <= 0 {
			continue
		}
		units, sub := packedQuantity/quantityLineScale, packedQuantity%quantityLineScale
		subDivisor := getIndex(e.DetailSubDivisor, lineIndex)
		if subDivisor <= 0 {
			subDivisor = quantityDivisorNone
		}
		amount := saturateInt32(int64(units)*int64(getIndex(e.DetailPrices, lineIndex)) + int64(sub)*int64(getIndex(e.DetailSubPrices, lineIndex)))
		line := SaleSummaryFrame{
			Date: e.Date, ProductID: productID, SubDivisor: subDivisor,
			AmountPEN: e.AmountInPEN(amount), SoldUnits: units, SoldSub: sub,
		}
		if isDeliveryPending {
			line.PendingUnits, line.PendingSub = units, sub
		}
		if isPaymentPending {
			line.DebtAmountPEN = line.AmountPEN
		}
		lines = append(lines, line)
	}
	return lines
}

// AmountInPEN is genix's finance.ConvertUnitPrice from USD to PEN.
func (e *SaleOrder) AmountInPEN(amount int32) int32 {
	if e.CurrencyType != currencyUSD || amount == 0 {
		return amount
	}
	return int32(math.Round(float64(amount) * float64(e.ExchangeRate) / exchangeRateScale))
}

var salesProjection = dataframe.ProjectionOf((*SaleOrder).ToSaleSummaryFrames)

func compileSales() *dataframe.Table {
	return mustCompile("sale_order", 25, []dataframe.Spec{{
		Name: "sale-summary", Version: 1, UseZeroSumsMask: true,
		CompactOn:  dataframe.CompactOn{Records: 100, Seconds: 3600},
		Projection: salesProjection,
	}})
}

// ── enrollment: a column frame, berryapps' shape ──

// Enrollment is one employee's training in a program. Status 0 is a soft delete.
type Enrollment struct {
	CompanyID    int32
	ID           int64
	Year         int16
	ProgramID    int32
	DepartmentID int32
	TeamID       int32
	EmployeeID   int32
	Minutes      int32
	Points       int32
	Hours        float64
	Status       int8
	Comment      string
	Updated      int32
}

var (
	enrollmentYear       = dataframe.Column{FieldName: "Year", ColumnID: 3}
	enrollmentProgram    = dataframe.Column{FieldName: "ProgramID", ColumnID: 4}
	enrollmentDepartment = dataframe.Column{FieldName: "DepartmentID", ColumnID: 5}
	enrollmentTeam       = dataframe.Column{FieldName: "TeamID", ColumnID: 6}
	enrollmentEmployee   = dataframe.Column{FieldName: "EmployeeID", ColumnID: 7}
	enrollmentMinutes    = dataframe.Column{FieldName: "Minutes", ColumnID: 8}
	enrollmentPoints     = dataframe.Column{FieldName: "Points", ColumnID: 9}
	enrollmentHours      = dataframe.Column{FieldName: "Hours", ColumnID: 10, IsFloat: true}
)

// enrollmentLinesOf builds the one line as scylla's columnLinesOf does: a Line and a []Line per record,
// floats × FloatScale.
func enrollmentLinesOf(recordPointer unsafe.Pointer) ([]dataframe.Line, error) {
	record := (*Enrollment)(recordPointer)
	if record.Status == 0 {
		return nil, nil
	}
	hours, _ := dataframe.ScaledFloat(record.Hours)
	line := dataframe.Line{int64(record.Year), int64(record.ProgramID), int64(record.DepartmentID), int64(record.TeamID), int64(record.EmployeeID),
		int64(record.Minutes), int64(record.Points), hours}
	return []dataframe.Line{line}, nil
}

func compileEnrollment() *dataframe.Table {
	return mustCompile("enrollment", 78901377, []dataframe.Spec{{
		Name: "year-program", Keys: []dataframe.Column{enrollmentYear, enrollmentProgram},
		Rows: []dataframe.Column{enrollmentDepartment, enrollmentTeam, enrollmentEmployee},
		Sums: []dataframe.Column{enrollmentMinutes, enrollmentPoints, enrollmentHours}, Count: true,
		LinesOf: enrollmentLinesOf,
	}})
}

// ── movements: a stock ledger, ToFrames with floats and two Keys ──

// StockMovement is one warehouse document moving several products and lots.
type StockMovement struct {
	CompanyID   int32
	ID          int64
	WarehouseID int32
	Week        int16 // weeks since 2020
	Kind        int8  // 0 annulled, 1 in, 2 out
	ProductIDs  []int32
	LotIDs      []int32
	Quantities  []float64
	UnitCosts   []float64
	Notes       string
	Updated     int32
}

// MovementFrame is one product and lot of a movement.
type MovementFrame struct {
	WarehouseID int32   `frame:"key"`
	Week        int16   `frame:"key"`
	ProductID   int32   `frame:"row"`
	LotID       int32   `frame:"row"`
	QuantityIn  float64 `frame:"sum"`
	QuantityOut float64 `frame:"sum"`
	Cost        float64 `frame:"sum"`
	CostCents   int64   `frame:"sum"`
}

func (movement *StockMovement) ToMovementFrames() []MovementFrame {
	if movement.Kind == 0 {
		return nil
	}
	lines := make([]MovementFrame, 0, len(movement.ProductIDs))
	for i, productID := range movement.ProductIDs {
		quantity, unitCost := movement.Quantities[i], movement.UnitCosts[i]
		line := MovementFrame{WarehouseID: movement.WarehouseID, Week: movement.Week, ProductID: productID, LotID: movement.LotIDs[i],
			Cost: quantity * unitCost, CostCents: int64(math.Round(quantity * unitCost * 100))}
		if movement.Kind == 1 {
			line.QuantityIn = quantity
		} else {
			line.QuantityOut = quantity
		}
		lines = append(lines, line)
	}
	return lines
}

var movementsProjection = dataframe.ProjectionOf((*StockMovement).ToMovementFrames)

func compileMovements() *dataframe.Table {
	return mustCompile("stock_movement", 41, []dataframe.Spec{{
		Name: "warehouse-week", Version: 1, UseZeroSumsMask: true, Count: true,
		Projection: movementsProjection,
	}})
}

func mustCompile(name string, tableID uint64, specs []dataframe.Spec) *dataframe.Table {
	table, err := dataframe.Compile(name, tableID, specs)
	if err != nil {
		panic(err)
	}
	return table
}
