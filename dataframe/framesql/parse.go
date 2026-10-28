package framesql

import (
	"fmt"
	"strconv"
	"strings"
)

// ─────────────────────────────────────────────────────────────────────────────
// The closed subset FrameSQL reads (../../DATA_FRAMES_SQL.md, section 4):
//
//	SELECT  item [, item]...          item := expression [[AS] alias]
//	[FROM   frame]                    day_product or day-product; Run's frameName when left out
//	WHERE   condition [AND condition]...
//	[GROUP BY expression [, ...]]     a column, WEEK(day), MONTH(day), or an item's position
//	[ORDER BY | SORT BY expression [ASC|DESC] [, ...]]
//	[LIMIT n] [;]
//
//	condition  := column = value | column IN (value, ...) | column IN PERIOD("<period>")
//	            | column BETWEEN integer AND integer
//	value      := integer | "name in quotes" | 'name in quotes'
//	expression := term {(+|-) term},  term := factor {(*|/) factor}
//	factor     := number | column | FUNCTION(* | expression) | (expression)
//
// Keywords and names are case-insensitive. Whatever SQL has beyond this fails with a message that
// says how to write it here.
// ─────────────────────────────────────────────────────────────────────────────

type statement struct {
	items      []selectItem
	frameName  string
	conditions []condition
	groupBy    []*expr
	orderBy    []orderTerm
	// limit is 0 when the statement has no LIMIT.
	limit int
}

type selectItem struct {
	value *expr
	alias string
}

type orderTerm struct {
	value        *expr
	isDescending bool
}

type exprKind uint8

const (
	exprColumn exprKind = iota
	exprCall
	exprNumber
	exprBinary
)

// expr is one node of an expression: a column, a function call, a number or an arithmetic operation.
type expr struct {
	kind exprKind
	// name is a column's name in lowercase, or a function's in uppercase.
	name   string
	isStar bool // COUNT(*)
	args   []*expr
	// number is a literal's value; text is how it was written.
	number    float64
	isDecimal bool
	text      string
	op        byte // + - * /
	left      *expr
	right     *expr
}

type conditionOp uint8

const (
	conditionEq conditionOp = iota
	conditionIn
	conditionBetween
	conditionPeriod
)

type condition struct {
	column string
	op     conditionOp
	// values are 1 for Eq, 2 for Between, the list for In.
	values []literal
	// period is the text inside PERIOD("…").
	period string
}

// literal is a condition's value: an integer, or a record's name in quotes.
type literal struct {
	integer int64
	name    string
	isName  bool
}

type tokenKind uint8

const (
	tokenEnd tokenKind = iota
	tokenWord
	tokenNumber
	tokenString
	tokenSymbol
)

type token struct {
	kind tokenKind
	// text is the token as written; a string's content without its quotes.
	text string
}

// describe names a token in an error message.
func (t token) describe() string {
	switch t.kind {
	case tokenEnd:
		return "the end of the statement"
	case tokenString:
		return `"` + t.text + `"`
	default:
		return "`" + t.text + "`"
	}
}

// reservedWords end an expression: a word among them is never a column or an implicit alias.
var reservedWords = map[string]bool{
	"SELECT": true, "FROM": true, "WHERE": true, "AND": true, "GROUP": true, "BY": true, "ORDER": true,
	"SORT": true, "LIMIT": true, "AS": true, "ASC": true, "DESC": true, "IN": true, "BETWEEN": true,
}

// unsupportedWords are SQL this subset doesn't take, with the way to write it here.
var unsupportedWords = map[string]string{
	"JOIN":     "a statement reads one frame: no joins",
	"INNER":    "a statement reads one frame: no joins",
	"LEFT":     "a statement reads one frame: no joins",
	"RIGHT":    "a statement reads one frame: no joins",
	"FULL":     "a statement reads one frame: no joins",
	"CROSS":    "a statement reads one frame: no joins",
	"UNION":    "one statement per call: no UNION",
	"HAVING":   "HAVING is not supported: filter Keys and Rows columns in WHERE, then use ORDER BY and LIMIT",
	"OFFSET":   "OFFSET is not supported: use ORDER BY and LIMIT",
	"DISTINCT": "DISTINCT is not supported: GROUP BY the columns instead",
	"OR":       "OR is not supported: use IN (…) for several values of one column",
	"NOT":      "NOT is not supported: list the values you want with IN (…)",
	"LIKE":     "LIKE is not supported: write the record's name in quotes, column = \"name\", and it is looked up",
	"IS":       "IS NULL is not supported: frames hold no NULL",
	"CASE":     "CASE is not supported",
	"WITH":     "WITH is not supported: write one SELECT",
}

func tokenize(statementText string) ([]token, error) {
	var tokens []token
	for position := 0; position < len(statementText); {
		char := statementText[position]
		switch {
		case char == ' ' || char == '\t' || char == '\n' || char == '\r':
			position++
		case isWordStart(char):
			end := position + 1
			for end < len(statementText) && (isWordStart(statementText[end]) || isDigit(statementText[end])) {
				end++
			}
			tokens = append(tokens, token{kind: tokenWord, text: statementText[position:end]})
			position = end
		case isDigit(char):
			end := position + 1
			for end < len(statementText) && (isDigit(statementText[end]) || statementText[end] == '.') {
				end++
			}
			tokens = append(tokens, token{kind: tokenNumber, text: statementText[position:end]})
			position = end
		case char == '\'' || char == '"':
			// A name in single or double quotes; its own quote inside it is written twice: 'O''Brien'.
			var content strings.Builder
			end := position + 1
			for {
				if end >= len(statementText) {
					return nil, fmt.Errorf("a name in quotes is not closed: add the closing %c", char)
				}
				if statementText[end] == char {
					if end+1 < len(statementText) && statementText[end+1] == char {
						content.WriteByte(char)
						end += 2
						continue
					}
					break
				}
				content.WriteByte(statementText[end])
				end++
			}
			tokens = append(tokens, token{kind: tokenString, text: content.String()})
			position = end + 1
		case char == '`':
			return nil, fmt.Errorf("` quotes are not supported: write names of records in quotes (\"name\") and columns without quotes")
		case char == '.':
			return nil, fmt.Errorf("write columns without a prefix: `amount`, not `d.amount`")
		default:
			if position+1 < len(statementText) {
				switch pair := statementText[position : position+2]; pair {
				case "<=", ">=", "<>", "!=":
					tokens = append(tokens, token{kind: tokenSymbol, text: pair})
					position += 2
					continue
				}
			}
			if !strings.ContainsRune(",()*+-/=<>;", rune(char)) {
				return nil, fmt.Errorf("unexpected character %q", char)
			}
			tokens = append(tokens, token{kind: tokenSymbol, text: string(char)})
			position++
		}
	}
	return append(tokens, token{kind: tokenEnd}), nil
}

func isWordStart(char byte) bool {
	return char == '_' || (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z')
}

func isDigit(char byte) bool { return char >= '0' && char <= '9' }

type parser struct {
	tokens []token
	next   int
}

func (p *parser) peek() token { return p.tokens[p.next] }

func (p *parser) advance() token {
	current := p.tokens[p.next]
	if current.kind != tokenEnd {
		p.next++
	}
	return current
}

func (p *parser) isKeyword(keyword string) bool {
	return p.peek().kind == tokenWord && strings.EqualFold(p.peek().text, keyword)
}

func (p *parser) acceptKeyword(keyword string) bool {
	if p.isKeyword(keyword) {
		p.next++
		return true
	}
	return false
}

func (p *parser) isSymbol(symbol string) bool {
	return p.peek().kind == tokenSymbol && p.peek().text == symbol
}

func (p *parser) acceptSymbol(symbol string) bool {
	if p.isSymbol(symbol) {
		p.next++
		return true
	}
	return false
}

// unexpected is the error for the next token where expected should be. An SQL word this subset
// doesn't take gets its own message.
func (p *parser) unexpected(expected string) error {
	if p.peek().kind == tokenWord {
		if message, isUnsupported := unsupportedWords[strings.ToUpper(p.peek().text)]; isUnsupported {
			return fmt.Errorf("%s", message)
		}
	}
	return fmt.Errorf("expected %s, found %s", expected, p.peek().describe())
}

// isNameWord reports whether the next token is a word that can be a column or an alias.
func (p *parser) isNameWord() bool {
	upperText := strings.ToUpper(p.peek().text)
	return p.peek().kind == tokenWord && !reservedWords[upperText] && unsupportedWords[upperText] == ""
}

func parse(statementText string) (*statement, error) {
	tokens, err := tokenize(statementText)
	if err != nil {
		return nil, err
	}
	p := &parser{tokens: tokens}
	parsed := &statement{}
	if !p.acceptKeyword("SELECT") {
		return nil, p.unexpected("SELECT at the start of the statement")
	}
	for {
		item, err := p.parseSelectItem()
		if err != nil {
			return nil, err
		}
		parsed.items = append(parsed.items, item)
		if !p.acceptSymbol(",") {
			break
		}
	}

	// FROM may be left out: the caller names the frame.
	if p.acceptKeyword("FROM") {
		if parsed.frameName, err = p.parseFrameName(); err != nil {
			return nil, err
		}
		if p.isSymbol(",") {
			return nil, fmt.Errorf("a statement reads one frame: no joins")
		}
		if p.isNameWord() || p.isKeyword("AS") {
			return nil, fmt.Errorf("frames take no alias: write the columns without a prefix")
		}
	}

	if p.acceptKeyword("WHERE") {
		for {
			condition, err := p.parseCondition()
			if err != nil {
				return nil, err
			}
			parsed.conditions = append(parsed.conditions, condition)
			if !p.acceptKeyword("AND") {
				break
			}
		}
	}
	if p.acceptKeyword("GROUP") {
		if !p.acceptKeyword("BY") {
			return nil, p.unexpected("BY after GROUP")
		}
		for {
			group, err := p.parseExpression()
			if err != nil {
				return nil, err
			}
			parsed.groupBy = append(parsed.groupBy, group)
			if !p.acceptSymbol(",") {
				break
			}
		}
	}
	if p.isKeyword("ORDER") || p.isKeyword("SORT") {
		sortWord := strings.ToUpper(p.advance().text)
		if !p.acceptKeyword("BY") {
			return nil, p.unexpected("BY after " + sortWord)
		}
		for {
			value, err := p.parseExpression()
			if err != nil {
				return nil, err
			}
			term := orderTerm{value: value, isDescending: p.acceptKeyword("DESC")}
			if !term.isDescending {
				p.acceptKeyword("ASC")
			}
			parsed.orderBy = append(parsed.orderBy, term)
			if !p.acceptSymbol(",") {
				break
			}
		}
	}
	if p.acceptKeyword("LIMIT") {
		limitToken := p.advance()
		limit, err := strconv.Atoi(limitToken.text)
		if limitToken.kind != tokenNumber || err != nil || limit < 1 {
			return nil, fmt.Errorf("LIMIT takes a whole number above 0, found %s", limitToken.describe())
		}
		parsed.limit = limit
	}
	p.acceptSymbol(";")
	if p.peek().kind != tokenEnd {
		return nil, p.unexpected("the end of the statement")
	}
	return parsed, nil
}

func (p *parser) parseSelectItem() (selectItem, error) {
	if p.isSymbol("*") {
		return selectItem{}, fmt.Errorf("SELECT * is not supported: list the columns and aggregates, such as SELECT product_id, SUM(amount)")
	}
	value, err := p.parseExpression()
	if err != nil {
		return selectItem{}, err
	}
	item := selectItem{value: value}
	hasAs := p.acceptKeyword("AS")
	switch {
	case p.peek().kind == tokenString && hasAs:
		item.alias = p.advance().text
	case p.isNameWord():
		item.alias = p.advance().text
	case hasAs:
		return selectItem{}, p.unexpected("an alias after AS")
	}
	return item, nil
}

// parseFrameName reads a frame's name: words joined by '-' or '_' (day-product, day_product).
func (p *parser) parseFrameName() (string, error) {
	if p.isSymbol("(") {
		return "", fmt.Errorf("subqueries are not supported: FROM takes a frame's name")
	}
	if p.peek().kind != tokenWord {
		return "", p.unexpected("a frame's name after FROM")
	}
	name := p.advance().text
	for p.isSymbol("-") {
		p.advance()
		if p.peek().kind != tokenWord && p.peek().kind != tokenNumber {
			return "", p.unexpected("the rest of the frame's name")
		}
		name += "-" + p.advance().text
	}
	return strings.ToLower(name), nil
}

func (p *parser) parseCondition() (condition, error) {
	if p.isSymbol("(") {
		return condition{}, fmt.Errorf("conditions take no parentheses: write column = value AND column IN (…)")
	}
	if !p.isNameWord() {
		return condition{}, p.unexpected("a column in WHERE")
	}
	parsed := condition{column: strings.ToLower(p.advance().text)}
	if p.isSymbol("(") {
		return condition{}, fmt.Errorf("conditions name a column, not a function: for a range of days write column IN PERIOD(\"…\")")
	}
	switch {
	case p.acceptSymbol("="):
		value, err := p.parseLiteral()
		if err != nil {
			return condition{}, err
		}
		parsed.op, parsed.values = conditionEq, []literal{value}
	case p.acceptKeyword("IN"):
		if p.acceptKeyword("PERIOD") {
			if !p.acceptSymbol("(") || p.peek().kind != tokenString {
				return condition{}, fmt.Errorf("write PERIOD with its notation in quotes: %s IN PERIOD(\"D-6..D\")", parsed.column)
			}
			parsed.op, parsed.period = conditionPeriod, p.advance().text
			if !p.acceptSymbol(")") {
				return condition{}, p.unexpected("`)` after the period")
			}
			return parsed, nil
		}
		if !p.acceptSymbol("(") {
			return condition{}, p.unexpected("`(` after IN")
		}
		if p.isKeyword("SELECT") {
			return condition{}, fmt.Errorf("subqueries are not supported: list the values, IN (1, 2) or IN (\"name\", \"other name\")")
		}
		parsed.op = conditionIn
		for {
			value, err := p.parseLiteral()
			if err != nil {
				return condition{}, err
			}
			parsed.values = append(parsed.values, value)
			if !p.acceptSymbol(",") {
				break
			}
		}
		if !p.acceptSymbol(")") {
			return condition{}, p.unexpected("`,` or `)` in the IN list")
		}
	case p.acceptKeyword("BETWEEN"):
		from, err := p.parseLiteral()
		if err != nil {
			return condition{}, err
		}
		if !p.acceptKeyword("AND") {
			return condition{}, p.unexpected("AND inside BETWEEN")
		}
		to, err := p.parseLiteral()
		if err != nil {
			return condition{}, err
		}
		parsed.op, parsed.values = conditionBetween, []literal{from, to}
	case p.peek().kind == tokenSymbol && strings.Contains("< > <= >= <> !=", p.peek().text):
		return condition{}, fmt.Errorf("`%s` is not supported: use =, IN (…), BETWEEN a AND b, or IN PERIOD(\"…\") for days", p.peek().text)
	default:
		return condition{}, p.unexpected("=, IN or BETWEEN after " + parsed.column)
	}
	return parsed, nil
}

// parseLiteral reads a condition's value: a whole number, or a record's name in quotes.
func (p *parser) parseLiteral() (literal, error) {
	switch current := p.peek(); {
	case current.kind == tokenString:
		p.advance()
		return literal{name: current.text, isName: true}, nil
	case current.kind == tokenNumber:
		p.advance()
		integer, err := strconv.ParseInt(current.text, 10, 64)
		if err != nil {
			return literal{}, fmt.Errorf("conditions compare whole numbers: %s is not one", current.text)
		}
		return literal{integer: integer}, nil
	case current.kind == tokenWord && strings.EqualFold(current.text, "PERIOD"):
		return literal{}, fmt.Errorf("write a period as column IN PERIOD(\"…\")")
	default:
		return literal{}, p.unexpected("a whole number or a name in quotes")
	}
}

func (p *parser) parseExpression() (*expr, error) {
	left, err := p.parseTerm()
	for err == nil && (p.isSymbol("+") || p.isSymbol("-")) {
		op := p.advance().text[0]
		var right *expr
		if right, err = p.parseTerm(); err == nil {
			left = &expr{kind: exprBinary, op: op, left: left, right: right}
		}
	}
	return left, err
}

func (p *parser) parseTerm() (*expr, error) {
	left, err := p.parseFactor()
	for err == nil && (p.isSymbol("*") || p.isSymbol("/")) {
		op := p.advance().text[0]
		var right *expr
		if right, err = p.parseFactor(); err == nil {
			left = &expr{kind: exprBinary, op: op, left: left, right: right}
		}
	}
	return left, err
}

func (p *parser) parseFactor() (*expr, error) {
	current := p.peek()
	switch {
	case p.acceptSymbol("("):
		if p.isKeyword("SELECT") {
			return nil, fmt.Errorf("subqueries are not supported")
		}
		inner, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		if !p.acceptSymbol(")") {
			return nil, p.unexpected("`)`")
		}
		return inner, nil
	case current.kind == tokenNumber:
		p.advance()
		number, err := strconv.ParseFloat(current.text, 64)
		if err != nil {
			return nil, fmt.Errorf("%s is not a number", current.text)
		}
		return &expr{kind: exprNumber, number: number, isDecimal: strings.Contains(current.text, "."), text: current.text}, nil
	case current.kind == tokenString:
		return nil, fmt.Errorf("a name in quotes goes in WHERE, column = \"%s\"", current.text)
	case p.isSymbol("-"):
		return nil, fmt.Errorf("negative numbers are not supported")
	case !p.isNameWord():
		return nil, p.unexpected("a column, a function or a number")
	}
	p.advance()
	if !p.acceptSymbol("(") {
		return &expr{kind: exprColumn, name: strings.ToLower(current.text)}, nil
	}
	call := &expr{kind: exprCall, name: strings.ToUpper(current.text)}
	if p.acceptSymbol("*") {
		call.isStar = true
	} else {
		for !p.isSymbol(")") {
			argument, err := p.parseExpression()
			if err != nil {
				return nil, err
			}
			call.args = append(call.args, argument)
			if !p.acceptSymbol(",") {
				break
			}
		}
	}
	if !p.acceptSymbol(")") {
		return nil, p.unexpected("`)` after the arguments of " + call.name)
	}
	return call, nil
}

// formatExpr writes an expression back as SQL, one way per tree: it names unaliased items and tells
// two items apart. A nested operation is parenthesized.
func formatExpr(node *expr) string {
	switch node.kind {
	case exprColumn:
		return node.name
	case exprNumber:
		return node.text
	case exprCall:
		if node.isStar {
			return node.name + "(*)"
		}
		args := make([]string, len(node.args))
		for i, argument := range node.args {
			args[i] = formatExpr(argument)
		}
		return node.name + "(" + strings.Join(args, ", ") + ")"
	default:
		return formatOperand(node.left) + " " + string(node.op) + " " + formatOperand(node.right)
	}
}

func formatOperand(node *expr) string {
	if node.kind == exprBinary {
		return "(" + formatExpr(node) + ")"
	}
	return formatExpr(node)
}
