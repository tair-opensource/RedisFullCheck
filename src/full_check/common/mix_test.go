package common

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseDBMap(t *testing.T) {
	var nr int

	// no mapping
	{
		nr++
		fmt.Printf("TestParseDBMap case %d.\n", nr)

		dbMap, err := ParseDBMap("")
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 0, len(dbMap), "should be equal")

		dbMap, err = ParseDBMap("-1")
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, 0, len(dbMap), "should be equal")
	}

	// single mapping
	{
		nr++
		fmt.Printf("TestParseDBMap case %d.\n", nr)

		dbMap, err := ParseDBMap("313:1")
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, map[int]int{313: 1}, dbMap, "should be equal")
	}

	// multiple mappings, the same source db mapped to the same target db is allowed
	{
		nr++
		fmt.Printf("TestParseDBMap case %d.\n", nr)

		dbMap, err := ParseDBMap("113:113;213:213;313:1;413:2")
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, map[int]int{113: 113, 213: 213, 313: 1, 413: 2}, dbMap, "should be equal")

		dbMap, err = ParseDBMap("313:1;313:1")
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, map[int]int{313: 1}, dbMap, "should be equal")
	}

	// spaces and empty items are tolerated
	{
		nr++
		fmt.Printf("TestParseDBMap case %d.\n", nr)

		dbMap, err := ParseDBMap(" 313 : 1 ;; 413:2 ;")
		assert.Equal(t, nil, err, "should be equal")
		assert.Equal(t, map[int]int{313: 1, 413: 2}, dbMap, "should be equal")
	}

	// invalid: no colon
	{
		nr++
		fmt.Printf("TestParseDBMap case %d.\n", nr)

		_, err := ParseDBMap("313-1")
		assert.NotEqual(t, nil, err, "should be equal")
	}

	// invalid: too many colons
	{
		nr++
		fmt.Printf("TestParseDBMap case %d.\n", nr)

		_, err := ParseDBMap("313:1:2")
		assert.NotEqual(t, nil, err, "should be equal")
	}

	// invalid: not an integer
	{
		nr++
		fmt.Printf("TestParseDBMap case %d.\n", nr)

		_, err := ParseDBMap("abc:1")
		assert.NotEqual(t, nil, err, "should be equal")

		_, err = ParseDBMap("313:abc")
		assert.NotEqual(t, nil, err, "should be equal")
	}

	// invalid: negative db
	{
		nr++
		fmt.Printf("TestParseDBMap case %d.\n", nr)

		_, err := ParseDBMap("-1:1")
		assert.NotEqual(t, nil, err, "should be equal")

		_, err = ParseDBMap("313:-1")
		assert.NotEqual(t, nil, err, "should be equal")
	}

	// invalid: one source db mapped to different target dbs
	{
		nr++
		fmt.Printf("TestParseDBMap case %d.\n", nr)

		_, err := ParseDBMap("313:1;313:2")
		assert.NotEqual(t, nil, err, "should be equal")
	}

	// invalid: comma is not the splitter
	{
		nr++
		fmt.Printf("TestParseDBMap case %d.\n", nr)

		_, err := ParseDBMap("313:1,413:2")
		assert.NotEqual(t, nil, err, "should be equal")
	}
}

func TestFilterDBList(t *testing.T) {
	var nr int

	// -1 means no filter
	{
		nr++
		fmt.Printf("TestFilterDBList case %d.\n", nr)

		dbList := FilterDBList("-1")
		assert.Equal(t, 0, len(dbList), "should be equal")
	}

	// normal
	{
		nr++
		fmt.Printf("TestFilterDBList case %d.\n", nr)

		dbList := FilterDBList("0;5;15")
		assert.Equal(t, map[int]struct{}{0: {}, 5: {}, 15: {}}, dbList, "should be equal")
	}

	// comma isn't the splitter, it panics
	{
		nr++
		fmt.Printf("TestFilterDBList case %d.\n", nr)

		assert.Panics(t, func() {
			FilterDBList("0,5")
		}, "should panic")
	}
}
