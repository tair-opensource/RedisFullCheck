package common

const (
	Star = byte('*')
)

type TrieNode struct {
	children map[byte]*TrieNode
	isEnd    bool
	isStar   bool // is ending by *
}

func newTrieNode() *TrieNode {
	return &TrieNode{children: make(map[byte]*TrieNode), isEnd: false, isStar: false}
}

type Trie struct {
	root *TrieNode
}

func NewTrie() *Trie {
	return &Trie{root: newTrieNode()}
}

func (trie *Trie) Insert(word []byte) {
	node := trie.root
	for _, char := range word {
		if char == Star {
			node.isStar = true
			break
		}

		_, ok := node.children[char]
		if !ok {
			node.children[char] = newTrieNode()
		}
		node = node.children[char]
	}
	node.isEnd = true
}

func (trie *Trie) Search(word []byte) bool {
	node := trie.root
	for _, char := range word {
		if node.isStar {
			return true
		}
		if _, ok := node.children[char]; !ok {
			return false
		}
		node = node.children[char]
	}
	return node.isEnd || node.isStar
}
