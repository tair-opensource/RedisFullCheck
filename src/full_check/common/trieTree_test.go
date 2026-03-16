package common

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTrie(t *testing.T) {
	var nr int

	// 测试精确匹配
	{
		nr++
		fmt.Printf("TestTrie case %d: 精确匹配.\n", nr)

		trie := NewTrie()
		insertList := []string{"abc", "def", "xyz"}
		for _, element := range insertList {
			trie.Insert([]byte(element))
		}

		assert.Equal(t, true, trie.Search([]byte("abc")), "精确匹配失败")
		assert.Equal(t, true, trie.Search([]byte("def")), "精确匹配失败")
		assert.Equal(t, true, trie.Search([]byte("xyz")), "精确匹配失败")
		assert.Equal(t, false, trie.Search([]byte("abcd")), "精确匹配错误")
		assert.Equal(t, false, trie.Search([]byte("xy")), "精确匹配错误")
	}

	// 测试前缀匹配
	{
		nr++
		fmt.Printf("TestTrie case %d: 前缀匹配.\n", nr)

		trie := NewTrie()
		insertList := []string{"abc*", "def*", "xyz*"}
		for _, element := range insertList {
			trie.Insert([]byte(element))
		}

		assert.Equal(t, true, trie.Search([]byte("abc")), "前缀匹配失败")
		assert.Equal(t, true, trie.Search([]byte("abc123")), "前缀匹配失败")
		assert.Equal(t, true, trie.Search([]byte("def456")), "前缀匹配失败")
		assert.Equal(t, false, trie.Search([]byte("ab")), "前缀匹配错误")
		assert.Equal(t, false, trie.Search([]byte("xy")), "前缀匹配错误")
	}

	// 测试后缀匹配
	{
		nr++
		fmt.Printf("TestTrie case %d: 后缀匹配.\n", nr)

		trie := NewTrie()
		insertList := []string{"*abc", "*def", "*xyz"}
		for _, element := range insertList {
			trie.Insert([]byte(element))
		}

		assert.Equal(t, true, trie.Search([]byte("123abc")), "后缀匹配失败")
		assert.Equal(t, true, trie.Search([]byte("456def")), "后缀匹配失败")
		assert.Equal(t, true, trie.Search([]byte("789xyz")), "后缀匹配失败")
		assert.Equal(t, false, trie.Search([]byte("abc123")), "后缀匹配错误")
		assert.Equal(t, false, trie.Search([]byte("def456")), "后缀匹配错误")
	}

	// 测试全匹配 *
	{
		nr++
		fmt.Printf("TestTrie case %d: 全匹配 *.\n", nr)

		trie := NewTrie()
		trie.Insert([]byte(""))

		assert.Equal(t, true, trie.Search([]byte("abc")), "全匹配失败")
		assert.Equal(t, true, trie.Search([]byte("123")), "全匹配失败")
		assert.Equal(t, true, trie.Search([]byte("xyz789")), "全匹配失败")
		assert.Equal(t, true, trie.Search([]byte("122111")), "全匹配失败")
	}

	// 测试空 filterlist
	{
		nr++
		fmt.Printf("TestCheckFilter case %d: 空 filterlist.\n", nr)

		var trie *Trie = nil // 空 filterlist
		assert.Equal(t, true, CheckFilter(trie, []byte("any_key")), "空 filterlist 匹配失败")
	}
}

func TestCheckBlock(t *testing.T) {
	var nr int

	// 测试 blocklist 匹配
	{
		nr++
		fmt.Printf("TestCheckBlock case %d: blocklist 匹配.\n", nr)

		trie := NewTrie()
		blockList := []string{"block1*", "*block2", "block3"}
		for _, element := range blockList {
			trie.Insert([]byte(element))
		}

		assert.Equal(t, true, CheckBlock(trie, []byte("block11")), "blocklist 匹配失败")
		assert.Equal(t, true, CheckBlock(trie, []byte("fblock2")), "blocklist 匹配失败")
		assert.Equal(t, true, CheckBlock(trie, []byte("block3")), "blocklist 匹配失败")
		assert.Equal(t, false, CheckBlock(trie, []byte("not_in_blocklist")), "blocklist 匹配错误")
	}

	// 测试空 blocklist
	{
		nr++
		fmt.Printf("TestCheckBlock case %d: 空 blocklist.\n", nr)

		var trie *Trie = nil // 空 blocklist
		assert.Equal(t, false, CheckBlock(trie, []byte("any_key")), "空 blocklist 匹配失败")
	}
}

func TestTrieRegex(t *testing.T) {
	var nr int

	nr++
	fmt.Printf("TestTrie case %d: 正则表达式匹配.\n", nr)

	trie := NewTrie()
	regexList := []string{"^test.*$", "^[0-9]+$", ".*end$"}
	for _, element := range regexList {
		trie.Insert([]byte(element))
	}

	// 匹配正则的
	assert.Equal(t, true, trie.Search([]byte("test123")), "正则表达式匹配失败")
	assert.Equal(t, true, trie.Search([]byte("12345")), "正则表达式匹配失败")
	assert.Equal(t, true, trie.Search([]byte("this_is_the_end")), "正则表达式匹配失败")

	// 不匹配正则的
	assert.Equal(t, false, trie.Search([]byte("example")), "正则表达式匹配错误")
	assert.Equal(t, false, trie.Search([]byte("abc123")), "正则表达式匹配错误")
	assert.Equal(t, false, trie.Search([]byte("start_middle")), "正则表达式匹配错误")
}
