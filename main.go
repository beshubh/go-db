package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"time"
)

func SaveData1(path string, data []byte) error {
	fp, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}

	defer fp.Close()
	_, err = fp.Write(data)
	if err != nil {
		return err
	}
	return fp.Sync()
}

func generateRandomNumber(min, max int) int {
	rand.New(rand.NewSource(time.Now().UnixMicro()))
	return rand.Intn(max-min+1) + min
}

func SaveData2(path string, data []byte) error {
	tmp := fmt.Sprintf("%s.tmp.%d", path, generateRandomNumber(0, 1000))
	fp, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer func() {
		fp.Close()
		if err != nil {
			os.Remove(tmp)
		}
	}()

	if _, err := fp.Write(data); err != nil {
		return err
	}

	if err := fp.Sync(); err != nil {
		return err
	}
	err = os.Rename(tmp, path)
	return err
}

func assert(cond bool, msg string) {
	if !cond {
		panic(msg)
	}
}

type Node struct {
	keys [][]byte
	// one of the following
	vals     [][]byte // for leaf nodes only
	children []*Node  // for internal nodes only
}

func Encode(node *Node) []byte

func Decode(page []byte) (*Node, error)

const (
	BNODE_NODE = 1 // internal nodes with pointers
	BNODE_LEAF = 2 // leaf nodes with values
)

const (
	BTREE_PAGE_SIZE    = 4096
	BTREE_MAX_KEY_SIZE = 1000
	BTREE_MAX_VAL_SIZE = 3000
)

func init() {
	node1Max := 4 + 1*8 + 1*2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE

	if node1Max > BTREE_PAGE_SIZE {
		panic("node1 too big")
	}
}

type BNode []byte

func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node[0:2])
}

func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node[2:4])
}

func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node[0:2], btype)
	binary.LittleEndian.PutUint16(node[2:4], nkeys)
}

func (node BNode) getPtr(idx uint16) uint64 {
	if idx >= node.nkeys() {
		panic("getPtr: invalid index")
	}
	pos := 4 + idx*8
	return binary.LittleEndian.Uint64(node[pos:])
}

func (node BNode) setPtr(idx uint16, val uint64) {
	if idx >= node.nkeys() {
		panic("setPtr: invalid index")
	}
	pos := 4 + idx*8
	binary.LittleEndian.PutUint64(node[pos:], val)
}

// we are doing idx-1 because the first offset is always 0
// and we never store it, its implicit
func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	pos := 4 + node.nkeys()*8 + 2*(idx-1)
	return binary.LittleEndian.Uint16(node[pos:])
}

func (node BNode) setOffset(idx uint16, val uint16) {
	binary.LittleEndian.PutUint16(node[4+node.nkeys()*8+2*(idx-1):], val)
}

func (node BNode) kvPos(idx uint16) uint16 {
	if idx > node.nkeys() {
		panic("kvPos: invalid index")
	}
	return 4 + node.nkeys()*8 + 2*node.nkeys() + node.getOffset(idx)
}

func (node BNode) getKey(idx uint16) []byte {
	if idx >= node.nkeys() {
		panic("getKey: invalid index")
	}
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos:])
	return node[pos+2:][:klen]
}

func (node BNode) getVal(idx uint16) []byte {
	if idx >= node.nkeys() {
		panic("getVal: invalid index")
	}
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos:])
	vlen := binary.LittleEndian.Uint16(node[pos+2:])
	return node[pos+4+klen:][:vlen]
}

// node size in bytes
func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys())
}

func nodeAppendKV(new BNode, idx uint16, ptr uint64, key []byte, val []byte) {
	new.setPtr(idx, ptr)

	pos := new.kvPos(idx)
	binary.LittleEndian.PutUint16(new[pos+0:], uint16(len(key)))
	binary.LittleEndian.PutUint16(new[pos+2:], uint16(len(val)))
	copy(new[pos+4:], key)
	copy(new[pos+4+uint16(len(key)):], val)
	new.setOffset(idx+1, new.getOffset(idx)+4+uint16((len(key)+len(val))))
}

func nodeAppendRange(new BNode, old BNode, dstNew uint16, srcOld uint16, n uint16) {
	for i := uint16(0); i < n; i++ {
		dst, src := dstNew+i, srcOld+i
		nodeAppendKV(new, dst, old.getPtr(src), old.getKey(src), old.getVal(src))
	}
}

func leafInsert(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	new.setHeader(BNODE_LEAF, old.nkeys()+1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, 0, key, val)
	nodeAppendRange(new, old, idx+1, idx, old.nkeys()-idx)
}

func leafUpdate(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	new.setHeader(BNODE_LEAF, old.nkeys())
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, 0, key, val)
	nodeAppendRange(new, old, idx+1, idx+1, old.nkeys()-idx-1)
}

func nodeLookupLE(node BNode, key []byte) uint16 {
	var i uint16
	for i = range node.nkeys() {
		cmp := bytes.Compare(node.getKey(i), key)
		if cmp == 0 {
			return i
		}

		if cmp > 0 {
			return i - 1
		}
	}
	return i - 1
}

func nodeSplit2(left BNode, right BNode, old BNode) {
	assert(old.nkeys() >= 2, "cannot split node with less than 2 keys")

	nleft := old.nkeys() / 2
	left_bytes := func() uint16 {
		return 4 + 8*nleft + 2*nleft + old.getOffset(nleft)
	}

	for left_bytes() > BTREE_PAGE_SIZE {
		nleft--
	}

	if nleft < 1 {
		panic("nleft is < 1, cannot split")
	}
	right_bytes := func() uint16 {
		return old.nbytes() - left_bytes() + 4
	}
	for right_bytes() > BTREE_PAGE_SIZE {
		nleft++
	}
	if nleft >= old.nkeys() {
		panic("nleft >= old.nkeys(), cannot split")
	}

	nright := old.nkeys() - nleft

	left.setHeader(BNODE_LEAF, nleft)
	right.setHeader(BNODE_LEAF, nright)
	nodeAppendRange(left, old, 0, 0, nleft)
	nodeAppendRange(right, old, 0, nleft, nright)
	assert(right.nbytes() <= BTREE_PAGE_SIZE, "right node too big")
	// left node might still be too big
}

// our size limits allow a single KV to take up almost the entire page
func nodeSplit3(old BNode) (uint16, [3]BNode) {
	if old.nbytes() <= BTREE_PAGE_SIZE {
		old = old[:BTREE_PAGE_SIZE]
		return 1, [3]BNode{old}
	}
	left := BNode(make([]byte, 2*BTREE_PAGE_SIZE))
	right := BNode(make([]byte, BTREE_PAGE_SIZE))
	nodeSplit2(left, right, old)
	if left.nbytes() <= BTREE_PAGE_SIZE {
		left = left[:BTREE_PAGE_SIZE]
		return 2, [3]BNode{left, right} // left & right fit in a single page
	}
	leftLeft := BNode(make([]byte, BTREE_PAGE_SIZE))
	leftRight := BNode(make([]byte, BTREE_PAGE_SIZE))
	nodeSplit2(leftLeft, leftRight, left)
	assert(leftLeft.nbytes() <= BTREE_PAGE_SIZE, "leftLeft too big")
	return 3, [3]BNode{leftLeft, leftRight, right}
}

type BTree struct {
	root uint64
	get  func(uint64) []byte // read data from page
	new  func([]byte) uint64 // allocate a new page with data
	del  func(uint64)        // delete a page
}

func treeInsert(tree *BTree, node BNode, key []byte, val []byte) BNode {
	new := BNode(make([]byte, 2*BTREE_PAGE_SIZE))
	idx := nodeLookupLE(node, key)

	switch node.btype() {
	case BNODE_LEAF:
		if bytes.Equal(key, node.getKey(idx)) {
			leafUpdate(new, node, idx, key, val)
		} else {
			leafInsert(new, node, idx, key, val)
		}

	case BNODE_NODE:
		childptr := node.getPtr(idx)
		childnode := treeInsert(tree, tree.get(childptr), key, val)

		// after insertion, the node might be too big, so we split
		nsplit, split := nodeSplit3(childnode)
		// delete the old child node
		tree.del(childptr)
		// update the child links
		nodeReplaceKidN(tree, new, node, idx, split[:nsplit]...)
	}

	return new
}

func nodeReplaceKidN(tree *BTree, new BNode, old BNode, idx uint16, kids ...BNode) {
	inc := uint16(len(kids))
	new.setHeader(BNODE_NODE, old.nkeys()+inc-1)
	nodeAppendRange(new, old, 0, 0, idx)

	for i, kid := range kids {
		nodeAppendKV(new, idx+uint16(i), tree.new(kid), kid.getKey(0), nil)
	}
	nodeAppendRange(new, old, idx+inc, idx+1, old.nkeys()-(idx+1))
}

func main() {
	// Copy-on-write means no inplace updates; updates create new nodes instead.
	// Example 1, node {"k1":"hi", "k2":"a", "k3":"hello"} is updated with "k2":"b":
	old := BNode(make([]byte, BTREE_PAGE_SIZE))
	old.setHeader(BNODE_LEAF, 2)
	nodeAppendKV(old, 0, 0, []byte("k1"), []byte("hi"))
	nodeAppendKV(old, 1, 0, []byte("k2"), []byte("hello"))

	new := BNode(make([]byte, BTREE_PAGE_SIZE))
	new.setHeader(BNODE_LEAF, 2)
	nodeAppendKV(new, 0, 0, old.getKey(0), old.getVal(0))
	nodeAppendKV(new, 1, 0, []byte("k2"), []byte("b"))
	nodeAppendKV(new, 2, 0, old.getKey(2), old.getVal(2))
	idx := nodeLookupLE(new, []byte("k2"))
	if bytes.Equal([]byte("k2"), new.getKey(idx)) {
		fmt.Println("found")
		// update here
	} else {
		// insert here
	}
}
