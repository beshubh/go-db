package main

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"time"
)

func main() {
	fmt.Println("hello world!")
}

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
		panic("invalid index")
	}
	pos := 4 + idx*8
	return binary.LittleEndian.Uint64(node[pos:])
}

func (node BNode) setPtr(idx uint16, val uint64) {
	if idx >= node.nkeys() {
		panic("invalid index")
	}
	pos := 4 + idx*8
	binary.LittleEndian.PutUint64(node[pos:], val)
}

func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	pos := 4 + node.nkeys()*8 + 2*(idx-1)
	return binary.LittleEndian.Uint16(node[pos:])
}

func (node BNode) kvPos(idx uint16) uint16 {
	if idx > node.nkeys() {
		panic("invalid index")
	}
	return 4 + node.nkeys()*8 + 2*node.nkeys() + node.getOffset(idx)
}

func (node BNode) getKey(idx uint16) []byte {
	if idx >= node.nkeys() {
		panic("invalid index")
	}
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos:])
	return node[pos+2:][:klen]
}

func (node BNode) getVal(idx uint16) []byte {
	if idx >= node.nkeys() {
		panic("invalid index")
	}
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos:])
	vlen := binary.LittleEndian.Uint16(node[pos+2:])
	return node[pos+4+klen:][:vlen]
}
