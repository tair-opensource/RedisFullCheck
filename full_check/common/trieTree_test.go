package common

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTrie(t *testing.T) {
	var nr int
	{
		nr++
		fmt.Printf("TestTrie case %d.\n", nr)

		trie := NewTrie()
		assert.Equal(t, false, trie.Search([]byte("abc")), "should be equal")
		assert.Equal(t, false, trie.Search([]byte("df")), "should be equal")
		assert.Equal(t, false, trie.Search([]byte("")), "should be equal")
	}

	{
		nr++
		fmt.Printf("TestTrie case %d.\n", nr)
		trie := NewTrie()
		insertList := []string{"abc", "adf" ,"bdf*", "m*"}
		for _, element := range insertList {
			trie.Insert([]byte(element))
		}

		assert.Equal(t, true, trie.Search([]byte("abc")), "should be equal")
		assert.Equal(t, false, trie.Search([]byte("abcd")), "should be equal")
		assert.Equal(t, false, trie.Search([]byte("adff")), "should be equal")
		assert.Equal(t, true, trie.Search([]byte("m")), "should be equal")
		assert.Equal(t, true, trie.Search([]byte("m1")), "should be equal")
		assert.Equal(t, false, trie.Search([]byte("")), "should be equal")
	}

	{
		nr++
		fmt.Printf("TestTrie case %d.\n", nr)
		trie := NewTrie()
		insertList := []string{"*"}
		for _, element := range insertList {
			trie.Insert([]byte(element))
		}

		assert.Equal(t, true, trie.Search([]byte("abc")), "should be equal")
		assert.Equal(t, true, trie.Search([]byte("abcd")), "should be equal")
		assert.Equal(t, true, trie.Search([]byte("adff")), "should be equal")
		assert.Equal(t, true, trie.Search([]byte("m")), "should be equal")
		assert.Equal(t, true, trie.Search([]byte("m1")), "should be equal")
		assert.Equal(t, true, trie.Search([]byte("")), "should be equal")
	}
}