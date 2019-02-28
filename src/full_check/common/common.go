package common

import "unsafe"

/*
 * @Vinllen Chen. check filter hit the key.
 * return: true/false. true means pass.
 * Actually, it's better to use trie tree instead of for-loop brute way. The reason I choose this is because
 * input filterList is not long in general, and I'm a lazy guy~.
 */
func CheckFilter(filterList *[]string, keyBytes []byte) bool {
	if len(*filterList) != 0 {
		//init on startup
		trie := initTrie(filterList)

		key := *(*string)(unsafe.Pointer(&keyBytes))
		return trie.Search([]rune(key))
	}
	return true // all pass when filter list is empty
}

func initTrie(filterList *[]string) *Trie {
	trie := NewTrie()
	for _, filterElement := range *filterList {
		trie.Insert([]rune(filterElement))
	}
	return trie
}

func isLeadingSame(a, b string, length int) bool {
	for i := 0; i < length; i++ {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}