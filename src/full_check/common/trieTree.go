package common

import (
	"regexp"
	"strings"
)

type TrieNode struct {
	children map[byte]*TrieNode
	isEnd    bool // 精确匹配
	isPrefix bool // 前缀匹配
	isSuffix bool // 后缀匹配
	isAll    bool // 全匹配（空串或 "*"）
}

func newTrieNode() *TrieNode {
	return &TrieNode{
		children: make(map[byte]*TrieNode),
	}
}

type Trie struct {
	root      *TrieNode
	regexList []*regexp.Regexp // 存储正则表达式
}

func NewTrie() *Trie {
	return &Trie{
		root:      newTrieNode(),
		regexList: make([]*regexp.Regexp, 0),
	}
}

func reverse(word []byte) []byte {
	for i, j := 0, len(word)-1; i < j; i, j = i+1, j-1 {
		word[i], word[j] = word[j], word[i]
	}
	return word
}

//func isRegex(str string) bool {
//	// 简单判断是否是正则表达式（也可以用更严谨的规则）
//	return strings.HasPrefix(str, "^") || strings.HasSuffix(str, "$") || strings.Contains(str, ".*")
//}

func isRegex(str string) bool {
	// 如果是前缀或后缀匹配（单个 * 在开头或结尾），不算正则表达式
	if strings.HasSuffix(str, "*") || strings.HasPrefix(str, "*") {
		return false
	}
	// 如果包含真正的正则符号，则认为是正则表达式
	return strings.ContainsAny(str, "^$+?.[](){}|\\")
}

func (trie *Trie) Insert(word []byte) {
	node := trie.root
	str := string(word)

	// 如果是正则表达式，直接加入 regexList
	if isRegex(str) {
		if re, err := regexp.Compile(str); err == nil {
			trie.regexList = append(trie.regexList, re)
		}
		return
	}

	// 全匹配
	if len(word) == 0 || (len(word) == 1 && word[0] == '*') {
		node.isAll = true
		return
	}

	// 前缀匹配（xxx*）
	if word[len(word)-1] == '*' {
		for _, char := range word[:len(word)-1] {
			if _, ok := node.children[char]; !ok {
				node.children[char] = newTrieNode()
			}
			node = node.children[char]
		}
		node.isPrefix = true
		return
	}

	// 后缀匹配（*xxx）
	if word[0] == '*' {
		reversedWord := reverse(word[1:])
		for _, char := range reversedWord {
			if _, ok := node.children[char]; !ok {
				node.children[char] = newTrieNode()
			}
			node = node.children[char]
		}
		node.isSuffix = true
		return
	}

	// 精确匹配
	for _, char := range word {
		if _, ok := node.children[char]; !ok {
			node.children[char] = newTrieNode()
		}
		node = node.children[char]
	}
	node.isEnd = true
}

func (trie *Trie) Search(word []byte) bool {
	node := trie.root

	if node.isAll {
		return true
	}

	// 精确匹配
	curr := node
	for _, char := range word {
		if _, ok := curr.children[char]; !ok {
			curr = nil
			break
		}
		curr = curr.children[char]
	}
	if curr != nil && curr.isEnd {
		return true
	}

	// 前缀匹配
	curr = node
	for _, char := range word {
		if curr.isPrefix {
			return true
		}
		if _, ok := curr.children[char]; !ok {
			curr = nil
			break
		}
		curr = curr.children[char]
	}
	if curr != nil && curr.isPrefix {
		return true
	}

	// 后缀匹配
	reversedWord := reverse(append([]byte(nil), word...))
	curr = node
	for _, char := range reversedWord {
		if curr.isSuffix {
			return true
		}
		if _, ok := curr.children[char]; !ok {
			curr = nil
			break
		}
		curr = curr.children[char]
	}
	if curr != nil && curr.isSuffix {
		return true
	}

	// 正则匹配
	for _, re := range trie.regexList {
		if re.Match(word) {
			return true
		}
	}

	return false
}
