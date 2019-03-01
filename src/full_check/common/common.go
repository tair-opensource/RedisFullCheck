package common

/*
 * @Vinllen Chen. check filter hit the key.
 * return: true/false. true means pass.
 * Actually, it's better to use trie tree instead of for-loop brute way. The reason I choose this is because
 * input filterList is not long in general, and I'm a lazy guy~.
 */
func CheckFilter(filterTree *Trie, keyBytes []byte) bool {
	if !filterTree.root.isEnd {
		return !filterTree.Search(keyBytes)

	}
	return true // all pass when filter list is empty
}

func isLeadingSame(a, b string, length int) bool {
	for i := 0; i < length; i++ {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}