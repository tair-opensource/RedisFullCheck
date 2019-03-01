package common

import (
	"fmt"
	"testing"
)

func TestCheckFilter(t *testing.T) {
	trieEmpty := NewTrie()
	fmt.Println("TireTree: ", "empty")
	fmt.Println("abc", "filter:",CheckFilter(trieEmpty, []byte("abc")))
	fmt.Println("df", "filter:",CheckFilter(trieEmpty, []byte("df")))

	fmt.Println("----------------")

	trie := NewTrie()
	fmt.Println("TireTree: ", "abc", "adf" ,"bdf*")
	trie.Insert([]byte("abc"))
	trie.Insert([]byte("adf"))
	trie.Insert([]byte("bdf*"))

	fmt.Println("abc", "filter:",CheckFilter(trie, []byte("abc")))
	fmt.Println("ada", "filter:",CheckFilter(trie, []byte("ada")))
	fmt.Println("ab", "filter:",CheckFilter(trie, []byte("ab")))
	fmt.Println("bdfff", "filter:",CheckFilter(trie, []byte("bdfff")))

}