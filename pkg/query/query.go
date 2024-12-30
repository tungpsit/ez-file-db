package query

import (
	"fmt"
	"reflect"
	"strings"
)

// Operator represents a comparison operator
type Operator string

const (
	Eq    Operator = "="
	Neq   Operator = "!="
	Gt    Operator = ">"
	Lt    Operator = "<"
	Gte   Operator = ">="
	Lte   Operator = "<="
	Like  Operator = "LIKE"
	In    Operator = "IN"
	NotIn Operator = "NOT IN"
)

// Condition represents a WHERE condition
type Condition struct {
	Column   string
	Operator Operator
	Value    interface{}
}

// Query represents a database query
type Query struct {
	Table      string
	Columns    []string
	Conditions []Condition
	OrderBy    []string
	Limit      int
	Offset     int
}

// NewQuery creates a new Query instance
func NewQuery(table string) *Query {
	return &Query{
		Table:   table,
		Columns: []string{"*"},
	}
}

// Select sets the columns to be retrieved
func (q *Query) Select(columns ...string) *Query {
	q.Columns = columns
	return q
}

// Where adds a condition to the query
func (q *Query) Where(column string, operator Operator, value interface{}) *Query {
	q.Conditions = append(q.Conditions, Condition{
		Column:   column,
		Operator: operator,
		Value:    value,
	})
	return q
}

// OrderByAsc adds ascending order by clause
func (q *Query) OrderByAsc(column string) *Query {
	q.OrderBy = append(q.OrderBy, column+" ASC")
	return q
}

// OrderByDesc adds descending order by clause
func (q *Query) OrderByDesc(column string) *Query {
	q.OrderBy = append(q.OrderBy, column+" DESC")
	return q
}

// SetLimit sets the limit clause
func (q *Query) SetLimit(limit int) *Query {
	q.Limit = limit
	return q
}

// SetOffset sets the offset clause
func (q *Query) SetOffset(offset int) *Query {
	q.Offset = offset
	return q
}

// Evaluate evaluates a record against the query conditions
func (q *Query) Evaluate(record map[string]interface{}) bool {
	for _, condition := range q.Conditions {
		value, exists := record[condition.Column]
		if !exists {
			return false
		}

		if !evaluateCondition(value, condition.Operator, condition.Value) {
			return false
		}
	}
	return true
}

// evaluateCondition evaluates a single condition
func evaluateCondition(value interface{}, operator Operator, target interface{}) bool {
	switch operator {
	case Eq:
		return reflect.DeepEqual(value, target)
	case Neq:
		return !reflect.DeepEqual(value, target)
	case Gt:
		return compareValues(value, target) > 0
	case Lt:
		return compareValues(value, target) < 0
	case Gte:
		return compareValues(value, target) >= 0
	case Lte:
		return compareValues(value, target) <= 0
	case Like:
		str, ok := value.(string)
		if !ok {
			return false
		}
		pattern, ok := target.(string)
		if !ok {
			return false
		}
		return strings.Contains(strings.ToLower(str), strings.ToLower(pattern))
	case In:
		targetSlice, ok := target.([]interface{})
		if !ok {
			return false
		}
		for _, t := range targetSlice {
			if reflect.DeepEqual(value, t) {
				return true
			}
		}
		return false
	case NotIn:
		targetSlice, ok := target.([]interface{})
		if !ok {
			return false
		}
		for _, t := range targetSlice {
			if reflect.DeepEqual(value, t) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

// compareValues compares two values
func compareValues(a, b interface{}) int {
	switch v1 := a.(type) {
	case int:
		v2, ok := b.(int)
		if !ok {
			return 0
		}
		if v1 < v2 {
			return -1
		}
		if v1 > v2 {
			return 1
		}
		return 0
	case float64:
		v2, ok := b.(float64)
		if !ok {
			return 0
		}
		if v1 < v2 {
			return -1
		}
		if v1 > v2 {
			return 1
		}
		return 0
	case string:
		v2, ok := b.(string)
		if !ok {
			return 0
		}
		return strings.Compare(v1, v2)
	default:
		return 0
	}
}

// String returns a string representation of the query
func (q *Query) String() string {
	var builder strings.Builder

	// SELECT clause
	builder.WriteString("SELECT ")
	builder.WriteString(strings.Join(q.Columns, ", "))

	// FROM clause
	builder.WriteString(" FROM ")
	builder.WriteString(q.Table)

	// WHERE clause
	if len(q.Conditions) > 0 {
		builder.WriteString(" WHERE ")
		conditions := make([]string, len(q.Conditions))
		for i, cond := range q.Conditions {
			conditions[i] = fmt.Sprintf("%s %s %v", cond.Column, cond.Operator, cond.Value)
		}
		builder.WriteString(strings.Join(conditions, " AND "))
	}

	// ORDER BY clause
	if len(q.OrderBy) > 0 {
		builder.WriteString(" ORDER BY ")
		builder.WriteString(strings.Join(q.OrderBy, ", "))
	}

	// LIMIT and OFFSET
	if q.Limit > 0 {
		builder.WriteString(fmt.Sprintf(" LIMIT %d", q.Limit))
	}
	if q.Offset > 0 {
		builder.WriteString(fmt.Sprintf(" OFFSET %d", q.Offset))
	}

	return builder.String()
}
