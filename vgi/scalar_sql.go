// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package vgi

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/apache/arrow-go/v18/arrow"
)

// A bounded parser for the two DuckDB scalar SQL grammars the DuckDB vgi
// extension accepts on read: CHECK constraint records (ParseNumericCheck in
// src/vgi_constraint_contract.cpp) and VIRTUAL generated columns (Scalar with
// generated=true in src/vgi_column_expression.cpp). It accepts a subset of
// DuckDB syntax and rejects everything outside the extension's grammar, so a
// table the SDK serves is not refused at ATTACH. It never evaluates SQL and
// cannot check bound result types; the extension still binds the expression.

type sqlTokenKind int

const (
	tokEOF sqlTokenKind = iota
	tokIdent
	tokQuotedIdent
	tokNumber
	tokString
	tokOp
)

type sqlToken struct {
	kind sqlTokenKind
	text string // identifiers: as written (quoted ones unescaped); ops: the operator
}

func tokenizeSQL(sql string) ([]sqlToken, error) {
	var out []sqlToken
	for i := 0; i < len(sql); {
		r, size := utf8.DecodeRuneInString(sql[i:])
		switch {
		case unicode.IsSpace(r):
			i += size
		case strings.HasPrefix(sql[i:], "--"):
			end := strings.IndexByte(sql[i:], '\n')
			if end < 0 {
				i = len(sql)
			} else {
				i += end + 1
			}
		case strings.HasPrefix(sql[i:], "/*"):
			end := strings.Index(sql[i+2:], "*/")
			if end < 0 {
				return nil, fmt.Errorf("unterminated comment")
			}
			i += end + 4
		case r == '"' || r == '\'':
			var b strings.Builder
			j := i + 1
			for {
				if j >= len(sql) {
					return nil, fmt.Errorf("unterminated quoted text")
				}
				if sql[j] == byte(r) {
					if j+1 < len(sql) && sql[j+1] == byte(r) {
						b.WriteByte(byte(r))
						j += 2
						continue
					}
					break
				}
				b.WriteByte(sql[j])
				j++
			}
			kind := tokString
			if r == '"' {
				kind = tokQuotedIdent
				if b.Len() == 0 {
					return nil, fmt.Errorf("empty quoted identifier")
				}
			}
			out = append(out, sqlToken{kind: kind, text: b.String()})
			i = j + 1
		case r >= '0' && r <= '9' || r == '.' && i+1 < len(sql) && sql[i+1] >= '0' && sql[i+1] <= '9':
			j := i
			for j < len(sql) && (sql[j] >= '0' && sql[j] <= '9' || sql[j] == '.' || sql[j] == '_') {
				j++
			}
			if j < len(sql) && (sql[j] == 'e' || sql[j] == 'E') {
				k := j + 1
				if k < len(sql) && (sql[k] == '+' || sql[k] == '-') {
					k++
				}
				if k < len(sql) && sql[k] >= '0' && sql[k] <= '9' {
					for k < len(sql) && sql[k] >= '0' && sql[k] <= '9' {
						k++
					}
					j = k
				}
			}
			out = append(out, sqlToken{kind: tokNumber, text: sql[i:j]})
			i = j
		case unicode.IsLetter(r) || r == '_':
			j := i
			for j < len(sql) {
				c, n := utf8.DecodeRuneInString(sql[j:])
				if !unicode.IsLetter(c) && !unicode.IsDigit(c) && c != '_' && c != '$' {
					break
				}
				j += n
			}
			out = append(out, sqlToken{kind: tokIdent, text: sql[i:j]})
			i = j
		default:
			op := ""
			for _, candidate := range []string{"::", "<=", ">=", "<>", "!=", "==", "||", "**", "//", "<<", ">>", "->", ":="} {
				if strings.HasPrefix(sql[i:], candidate) {
					op = candidate
					break
				}
			}
			if op == "" {
				op = string(r)
			}
			out = append(out, sqlToken{kind: tokOp, text: op})
			i += len(op)
		}
	}
	return append(out, sqlToken{kind: tokEOF}), nil
}

// sqlNode is a parsed scalar expression.
type sqlNode struct {
	kind     string // const, column, cast, compare, and, or, not, isnull, coalesce, func
	name     string // column name, function/operator name, cast type
	constant string // const: "integer", "bigint", "hugeint", "decimal", "double", "varchar", "boolean", "null"
	args     []*sqlNode
}

type sqlParser struct {
	tokens []sqlToken
	pos    int
	nodes  int
}

// Unquoted words DuckDB treats as keywords. Using one where an operand is
// expected is either a syntax error or an expression class outside both
// grammars (CASE, IN, LIKE, BETWEEN, subqueries, ...).
var sqlReservedWords = map[string]bool{
	"all": true, "and": true, "any": true, "array": true, "as": true, "asc": true, "between": true,
	"case": true, "cast": true, "collate": true, "desc": true, "distinct": true, "else": true, "end": true,
	"escape": true, "exists": true, "false": true, "filter": true, "from": true, "glob": true, "ilike": true,
	"in": true, "interval": true, "is": true, "isnull": true, "like": true, "not": true, "notnull": true,
	"null": true, "or": true, "order": true, "over": true, "select": true, "similar": true, "some": true,
	"then": true, "true": true, "try_cast": true, "when": true, "where": true, "with": true,
}

func (p *sqlParser) peek() sqlToken { return p.tokens[p.pos] }

func (p *sqlParser) next() sqlToken {
	t := p.tokens[p.pos]
	if t.kind != tokEOF {
		p.pos++
	}
	return t
}

func (p *sqlParser) isWord(word string) bool {
	t := p.peek()
	return t.kind == tokIdent && strings.EqualFold(t.text, word)
}

func (p *sqlParser) isOp(op string) bool {
	t := p.peek()
	return t.kind == tokOp && t.text == op
}

func (p *sqlParser) expectOp(op string) error {
	if !p.isOp(op) {
		return fmt.Errorf("expected %q", op)
	}
	p.next()
	return nil
}

func (p *sqlParser) node(kind, name string, args ...*sqlNode) (*sqlNode, error) {
	p.nodes++
	if p.nodes > 4096 {
		return nil, fmt.Errorf("scalar expression is too complex")
	}
	return &sqlNode{kind: kind, name: name, args: args}, nil
}

func parseScalarSQL(sql string) (*sqlNode, error) {
	if sql == "" || len(sql) > 1024*1024 || strings.ContainsRune(sql, 0) || !utf8.ValidString(sql) {
		return nil, fmt.Errorf("invalid SQL size or text")
	}
	tokens, err := tokenizeSQL(sql)
	if err != nil {
		return nil, err
	}
	p := &sqlParser{tokens: tokens}
	expr, err := p.parseOr(0)
	if err != nil {
		return nil, err
	}
	if t := p.peek(); t.kind != tokEOF {
		if t.kind == tokIdent && strings.EqualFold(t.text, "as") || t.kind == tokIdent && !sqlReservedWords[strings.ToLower(t.text)] {
			return nil, fmt.Errorf("aliases are unsupported")
		}
		return nil, fmt.Errorf("expected exactly one scalar expression, found %q", t.text)
	}
	return expr, nil
}

func (p *sqlParser) depthCheck(depth int) error {
	if depth > 64 {
		return fmt.Errorf("scalar expression is too complex")
	}
	return nil
}

func (p *sqlParser) parseOr(depth int) (*sqlNode, error) {
	left, err := p.parseAnd(depth)
	if err != nil {
		return nil, err
	}
	for p.isWord("or") {
		p.next()
		right, err := p.parseAnd(depth)
		if err != nil {
			return nil, err
		}
		if left, err = p.node("or", "or", left, right); err != nil {
			return nil, err
		}
	}
	return left, nil
}

func (p *sqlParser) parseAnd(depth int) (*sqlNode, error) {
	left, err := p.parseNot(depth)
	if err != nil {
		return nil, err
	}
	for p.isWord("and") {
		p.next()
		right, err := p.parseNot(depth)
		if err != nil {
			return nil, err
		}
		if left, err = p.node("and", "and", left, right); err != nil {
			return nil, err
		}
	}
	return left, nil
}

func (p *sqlParser) parseNot(depth int) (*sqlNode, error) {
	if err := p.depthCheck(depth); err != nil {
		return nil, err
	}
	if p.isWord("not") {
		p.next()
		operand, err := p.parseNot(depth + 1)
		if err != nil {
			return nil, err
		}
		return p.node("not", "not", operand)
	}
	return p.parseIs(depth)
}

func (p *sqlParser) parseIs(depth int) (*sqlNode, error) {
	left, err := p.parseCompare(depth)
	if err != nil {
		return nil, err
	}
	switch {
	case p.isWord("isnull"):
		p.next()
		return p.node("isnull", "is null", left)
	case p.isWord("notnull"):
		p.next()
		return p.node("isnull", "is not null", left)
	case p.isWord("is"):
		p.next()
		name := "is null"
		if p.isWord("not") {
			p.next()
			name = "is not null"
		}
		if !p.isWord("null") {
			return nil, fmt.Errorf("only IS [NOT] NULL is supported")
		}
		p.next()
		return p.node("isnull", name, left)
	}
	return left, nil
}

var sqlComparisons = map[string]string{"=": "=", "==": "=", "!=": "<>", "<>": "<>", "<": "<", ">": ">", "<=": "<=", ">=": ">="}

func (p *sqlParser) parseCompare(depth int) (*sqlNode, error) {
	left, err := p.parseOther(depth)
	if err != nil {
		return nil, err
	}
	if t := p.peek(); t.kind == tokOp {
		if op, ok := sqlComparisons[t.text]; ok {
			p.next()
			right, err := p.parseOther(depth)
			if err != nil {
				return nil, err
			}
			if t := p.peek(); t.kind == tokOp && sqlComparisons[t.text] != "" {
				return nil, fmt.Errorf("chained comparisons are not valid SQL")
			}
			return p.node("compare", op, left, right)
		}
	}
	if t := p.peek(); t.kind == tokIdent {
		switch strings.ToLower(t.text) {
		case "in", "like", "ilike", "glob", "similar", "between":
			return nil, fmt.Errorf("unsupported %s expression", strings.ToUpper(t.text))
		case "not":
			return nil, fmt.Errorf("unsupported NOT IN/LIKE/BETWEEN expression")
		}
	}
	return left, nil
}

// parseOther covers Postgres's generic operator level, which holds ||.
func (p *sqlParser) parseOther(depth int) (*sqlNode, error) {
	left, err := p.parseAdditive(depth)
	if err != nil {
		return nil, err
	}
	for p.isOp("||") {
		p.next()
		right, err := p.parseAdditive(depth)
		if err != nil {
			return nil, err
		}
		if left, err = p.node("func", "||", left, right); err != nil {
			return nil, err
		}
	}
	if t := p.peek(); t.kind == tokOp {
		switch t.text {
		case "^", "**", "//", "&", "|", "<<", ">>", "~", "!", "@", "->", "[", ":=":
			return nil, fmt.Errorf("unsupported operator %q", t.text)
		}
	}
	return left, nil
}

func (p *sqlParser) parseAdditive(depth int) (*sqlNode, error) {
	left, err := p.parseMultiplicative(depth)
	if err != nil {
		return nil, err
	}
	for p.isOp("+") || p.isOp("-") {
		op := p.next().text
		right, err := p.parseMultiplicative(depth)
		if err != nil {
			return nil, err
		}
		if left, err = p.node("func", op, left, right); err != nil {
			return nil, err
		}
	}
	return left, nil
}

func (p *sqlParser) parseMultiplicative(depth int) (*sqlNode, error) {
	left, err := p.parseUnary(depth)
	if err != nil {
		return nil, err
	}
	for p.isOp("*") || p.isOp("/") || p.isOp("%") {
		op := p.next().text
		right, err := p.parseUnary(depth)
		if err != nil {
			return nil, err
		}
		if left, err = p.node("func", op, left, right); err != nil {
			return nil, err
		}
	}
	return left, nil
}

func (p *sqlParser) parseUnary(depth int) (*sqlNode, error) {
	if err := p.depthCheck(depth); err != nil {
		return nil, err
	}
	if p.isOp("-") || p.isOp("+") {
		op := p.next().text
		operand, err := p.parseUnary(depth + 1)
		if err != nil {
			return nil, err
		}
		// The DuckDB (Postgres) grammar folds a minus applied to a numeric
		// literal into a negative constant; unary plus stays an operator.
		if op == "-" && operand.kind == "const" && isNumericConstant(operand.constant) {
			return operand, nil
		}
		return p.node("func", op, operand)
	}
	return p.parsePostfix(depth)
}

func (p *sqlParser) parsePostfix(depth int) (*sqlNode, error) {
	expr, err := p.parsePrimary(depth)
	if err != nil {
		return nil, err
	}
	for p.isOp("::") {
		p.next()
		target, err := p.parseTypeName()
		if err != nil {
			return nil, err
		}
		if expr, err = p.node("cast", target, expr); err != nil {
			return nil, err
		}
	}
	if p.isOp("[") || p.isOp(".") {
		return nil, fmt.Errorf("subscripts and qualified names are unsupported")
	}
	return expr, nil
}

func (p *sqlParser) parsePrimary(depth int) (*sqlNode, error) {
	if err := p.depthCheck(depth); err != nil {
		return nil, err
	}
	t := p.next()
	switch t.kind {
	case tokNumber:
		kind, err := numericConstantType(t.text)
		if err != nil {
			return nil, err
		}
		n, err := p.node("const", "")
		if err != nil {
			return nil, err
		}
		n.constant = kind
		return n, nil
	case tokString:
		n, err := p.node("const", "")
		if err != nil {
			return nil, err
		}
		n.constant = "varchar"
		return n, nil
	case tokQuotedIdent:
		if p.isOp("(") {
			return nil, fmt.Errorf("quoted function names are unsupported")
		}
		if p.isOp(".") {
			return nil, fmt.Errorf("qualified column references are unsupported")
		}
		return p.node("column", t.text)
	case tokOp:
		if t.text == "(" {
			inner, err := p.parseOr(depth + 1)
			if err != nil {
				return nil, err
			}
			if p.isOp(",") {
				return nil, fmt.Errorf("row constructors are unsupported")
			}
			return inner, p.expectOp(")")
		}
		if t.text == "*" {
			return nil, fmt.Errorf("star expressions are unsupported")
		}
		if t.text == "?" || t.text == "$" {
			return nil, fmt.Errorf("parameters are unsupported")
		}
		return nil, fmt.Errorf("unexpected %q", t.text)
	case tokIdent:
		word := strings.ToLower(t.text)
		switch word {
		case "null", "true", "false":
			n, err := p.node("const", "")
			if err != nil {
				return nil, err
			}
			n.constant = "boolean"
			if word == "null" {
				n.constant = "null"
			}
			return n, nil
		case "cast":
			if err := p.expectOp("("); err != nil {
				return nil, err
			}
			operand, err := p.parseOr(depth + 1)
			if err != nil {
				return nil, err
			}
			if !p.isWord("as") {
				return nil, fmt.Errorf("expected AS in CAST")
			}
			p.next()
			target, err := p.parseTypeName()
			if err != nil {
				return nil, err
			}
			if err := p.expectOp(")"); err != nil {
				return nil, err
			}
			return p.node("cast", target, operand)
		case "try_cast":
			return nil, fmt.Errorf("TRY_CAST is unsupported")
		}
		if p.isOp("(") {
			p.next()
			var args []*sqlNode
			if p.isWord("distinct") || p.isWord("all") || p.isOp("*") {
				return nil, fmt.Errorf("aggregate syntax is unsupported")
			}
			if !p.isOp(")") {
				for {
					if t := p.peek(); (t.kind == tokIdent || t.kind == tokQuotedIdent) && p.tokens[p.pos+1].kind == tokOp &&
						(p.tokens[p.pos+1].text == ":=" || p.tokens[p.pos+1].text == "=>") {
						return nil, fmt.Errorf("named scalar arguments are unsupported")
					}
					arg, err := p.parseOr(depth + 1)
					if err != nil {
						return nil, err
					}
					args = append(args, arg)
					if p.isWord("order") {
						return nil, fmt.Errorf("ORDER BY arguments are unsupported")
					}
					if !p.isOp(",") {
						break
					}
					p.next()
				}
			}
			if err := p.expectOp(")"); err != nil {
				return nil, err
			}
			if p.isWord("filter") || p.isWord("over") {
				return nil, fmt.Errorf("FILTER/OVER clauses are unsupported")
			}
			if word == "coalesce" {
				if len(args) == 0 {
					return nil, fmt.Errorf("COALESCE requires arguments")
				}
				return p.node("coalesce", "coalesce", args...)
			}
			return p.node("func", word, args...)
		}
		if sqlReservedWords[word] {
			return nil, fmt.Errorf("unsupported %s expression", strings.ToUpper(word))
		}
		if p.isOp(".") {
			return nil, fmt.Errorf("qualified column references are unsupported")
		}
		return p.node("column", t.text)
	}
	return nil, fmt.Errorf("expected a scalar expression")
}

// parseTypeName reads an unqualified cast target and returns its default-bound
// DuckDB type name, or "" for a type outside both grammars.
func (p *sqlParser) parseTypeName() (string, error) {
	t := p.next()
	if t.kind != tokIdent && t.kind != tokQuotedIdent {
		return "", fmt.Errorf("expected a type name")
	}
	if p.isOp(".") {
		return "", fmt.Errorf("qualified scalar cast types are unsupported")
	}
	name := strings.ToLower(t.text)
	if p.isOp("(") { // VARCHAR(n), DECIMAL(p, s)
		p.next()
		for !p.isOp(")") {
			if p.peek().kind == tokEOF {
				return "", fmt.Errorf("unterminated type parameters")
			}
			p.next()
		}
		p.next()
		if name == "decimal" || name == "numeric" {
			return "decimal", nil
		}
	}
	if p.isOp("[") {
		return "", fmt.Errorf("unsupported scalar cast")
	}
	switch name {
	case "boolean", "bool", "logical":
		return "boolean", nil
	case "tinyint", "int1":
		return "tinyint", nil
	case "smallint", "int2", "short":
		return "smallint", nil
	case "integer", "int", "int4", "signed":
		return "integer", nil
	case "bigint", "int8", "long":
		return "bigint", nil
	case "varchar", "text", "string", "char", "bpchar", "nvarchar":
		return "varchar", nil
	case "float", "float4", "real":
		return "float", nil
	case "double", "float8":
		return "double", nil
	case "decimal", "numeric":
		return "decimal", nil
	}
	return "", nil
}

func isNumericConstant(kind string) bool {
	switch kind {
	case "integer", "bigint", "hugeint", "decimal", "double":
		return true
	}
	return false
}

// numericConstantType mirrors how the DuckDB transformer types an unsigned
// numeric literal: INTEGER, BIGINT or HUGEINT for integers, DECIMAL for a
// dotted literal and DOUBLE for an exponent.
func numericConstantType(text string) (string, error) {
	clean := strings.ReplaceAll(text, "_", "")
	if strings.ContainsAny(clean, "eE") {
		if _, err := strconv.ParseFloat(clean, 64); err != nil {
			return "", fmt.Errorf("invalid numeric literal %q", text)
		}
		return "double", nil
	}
	if strings.Contains(clean, ".") {
		if strings.Count(clean, ".") > 1 {
			return "", fmt.Errorf("invalid numeric literal %q", text)
		}
		return "decimal", nil
	}
	value, err := strconv.ParseUint(clean, 10, 64)
	switch {
	case err != nil:
		return "hugeint", nil
	case value <= math.MaxInt32:
		return "integer", nil
	case value <= math.MaxInt64:
		return "bigint", nil
	}
	return "hugeint", nil
}

// findColumnCI resolves an unqualified SQL column reference the way DuckDB
// binds it: case-insensitively, rejecting an ambiguous spelling. rowID is
// the index of the is_row_id column (-1 for none).
func findColumnCI(columns *arrow.Schema, name string) (int, error) {
	index := -1
	if columns == nil {
		return -1, nil
	}
	for i, field := range columns.Fields() {
		if strings.EqualFold(field.Name, name) {
			if index >= 0 {
				return -1, fmt.Errorf("ambiguous column reference %q", name)
			}
			index = i
		}
	}
	return index, nil
}

// validateNumericCheckSQL mirrors ParseNumericCheck: exactly one comparison
// whose operands are non-NULL numeric literals or unqualified references to
// primitive signed numeric columns (INT8..INT64, FLOAT, DOUBLE) without an
// extension type. With nil columns only the shape is checked.
func validateNumericCheckSQL(sql string, columns *arrow.Schema) error {
	expr, err := parseScalarSQL(sql)
	if err != nil {
		return err
	}
	if expr.kind != "compare" {
		return fmt.Errorf("only a Boolean numeric comparison (column/constant op column/constant) is supported")
	}
	for _, operand := range expr.args {
		switch operand.kind {
		case "const":
			if !isNumericConstant(operand.constant) {
				return fmt.Errorf("CHECK requires numeric operands")
			}
		case "column":
			if columns == nil {
				continue
			}
			index, err := findColumnCI(columns, operand.name)
			if err != nil {
				return err
			}
			if index < 0 || isRowIDField(columns.Field(index)) {
				return fmt.Errorf("CHECK references a missing or synthetic column %q", operand.name)
			}
			field := columns.Field(index)
			if field.Metadata.FindKey("ARROW:extension:name") >= 0 || field.Type.ID() == arrow.EXTENSION {
				return fmt.Errorf("CHECK extension-typed operands are unsupported")
			}
			switch field.Type.ID() {
			case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64, arrow.FLOAT32, arrow.FLOAT64:
			default:
				return fmt.Errorf("CHECK requires primitive signed numeric columns; %q is %s", field.Name, field.Type)
			}
		default:
			return fmt.Errorf("only numeric column/constant comparisons are supported")
		}
	}
	return nil
}

// generatedPrimitiveArrow reports whether a field is one of the types the
// extension accepts for a generated column or its operands: Boolean, signed
// 8..64-bit integers or UTF-8 strings, without an extension type.
func generatedPrimitiveArrow(field arrow.Field) bool {
	if field.Metadata.FindKey("ARROW:extension:name") >= 0 || field.Type.ID() == arrow.EXTENSION {
		return false
	}
	switch field.Type.ID() {
	case arrow.BOOL, arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64, arrow.STRING:
		return true
	}
	return false
}

// validateGeneratedSQL mirrors the extension's generated-column grammar:
// literals of NULL/Boolean/integer/VARCHAR type, unqualified column references,
// CAST (not TRY_CAST) to those types, comparisons, AND/OR/NOT, IS [NOT] NULL,
// COALESCE, and the functions + - * % || abs lower upper. Referenced columns
// must exist, must not be the column itself and must have a generated-primitive
// type. With nil columns only the grammar is checked.
func validateGeneratedSQL(sql string, columns *arrow.Schema, self string) error {
	expr, err := parseScalarSQL(sql)
	if err != nil {
		return err
	}
	var walk func(*sqlNode) error
	walk = func(n *sqlNode) error {
		switch n.kind {
		case "const":
			switch n.constant {
			case "null", "boolean", "integer", "bigint", "varchar":
			default:
				return fmt.Errorf("unsupported scalar literal type %s", strings.ToUpper(n.constant))
			}
		case "column":
			if columns != nil {
				index, err := findColumnCI(columns, n.name)
				if err != nil {
					return err
				}
				if index < 0 || isRowIDField(columns.Field(index)) {
					return fmt.Errorf("generated expression references unknown column %q", n.name)
				}
				if strings.EqualFold(columns.Field(index).Name, self) {
					return fmt.Errorf("generated column %q cannot reference itself", self)
				}
				if !generatedPrimitiveArrow(columns.Field(index)) {
					return fmt.Errorf("unsupported generated operand type for column %q", columns.Field(index).Name)
				}
			}
		case "cast":
			switch n.name {
			case "boolean", "tinyint", "smallint", "integer", "bigint", "varchar":
			default:
				return fmt.Errorf("unsupported scalar cast")
			}
		case "compare", "and", "or", "not", "isnull", "coalesce":
		case "func":
			switch n.name {
			case "+", "-", "*", "%", "abs", "lower", "upper", "||":
			default:
				return fmt.Errorf("unsupported scalar function %q", n.name)
			}
		default:
			return fmt.Errorf("expression is outside the supported deterministic scalar grammar")
		}
		for _, arg := range n.args {
			if err := walk(arg); err != nil {
				return err
			}
		}
		return nil
	}
	return walk(expr)
}

func isRowIDField(field arrow.Field) bool {
	return field.Metadata.FindKey("is_row_id") >= 0
}
