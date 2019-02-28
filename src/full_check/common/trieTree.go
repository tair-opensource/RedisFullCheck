package common

type TrieNode struct {
	children map[rune]*TrieNode
	isEnd    bool
}

func newTrieNode() *TrieNode {
	return &TrieNode{children: make(map[rune]*TrieNode), isEnd: false}
}

type Trie struct {
	root *TrieNode
}

func NewTrie() *Trie {
	return &Trie{root: newTrieNode()}
}

func (trie *Trie) Insert(word []rune) {
	node := trie.root
	for i := 0; i < len(word); i++ {
		_, ok := node.children[word[i]]
		if !ok {
			node.children[word[i]] = newTrieNode()
		}
		node = node.children[word[i]]
	}
	node.isEnd = true
}

func (trie *Trie) Search(word []rune) bool {
	node := trie.root
	for i := 0; i < len(word); i++ {
		if _, ok := node.children['*']; ok {
			return true;
		}
		if _, ok := node.children[word[i]]; !ok {
			return false
		}
		node = node.children[word[i]]
	}
	return node.isEnd
}
