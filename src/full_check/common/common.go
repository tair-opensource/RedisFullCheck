package common

import "unsafe"

/*
 * @Vinllen Chen. check filter hit the key.
 * return: true/false. true means pass.
 * Actually, it's better to use trie tree instead of for-loop brute way. The reason I choose this is because
 * input filterList is not long in general, and I'm a lazy guy~.
 */
func CheckFilter(filterList []string, keyBytes []byte) bool {
	if len(filterList) != 0 {
		key := *(*string)(unsafe.Pointer(&keyBytes))
		for _, filterElement := range filterList {
			length := len(filterElement)
			if filterElement[length - 1] != '*' {
				// exact match
				if length == len(key) && filterElement == key {
					return true
				}
			} else {
				// prefix match
				if length - 1 <= len(key) && isLeadingSame(key, filterElement, length - 1) {
					return true
				}
			}
		}

		return false
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