// Copyright 2021 hardcore-os Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lsm

import (
	"encoding/binary"
	"fmt"
	"github.com/hardcore-os/corekv/file"
	"github.com/hardcore-os/corekv/utils"
	"github.com/hardcore-os/corekv/utils/codec/pb"
	"github.com/pkg/errors"
	"io"
	"math"
	"os"
	"sort"
)

// TODO LAB 这里实现 table
type table struct {
	ss  *file.SSTable
	lm  *levelManager
	fid uint64
}

//打开sst文件，需要把数据序列化，并且把序列化后的数据写入sst文件
func openTable(lm *levelManager, tableName string, builder *tableBuilder) *table {
	ss := file.OpenSStable(&file.Options{
		FileName: tableName,
		Dir:      lm.opt.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int(lm.opt.SSTableMaxSz),
	})
	t := &table{ss: ss, lm: lm, fid: utils.FID(tableName)}
	if builder != nil {
		if err := builder.flush(ss); err != nil {
			utils.Err(err)
			return nil
		}

	}
	if err := t.ss.Init(); err != nil {
		utils.Err(err)
		return nil
	}
	return t
}

// Serach 从table中查找key

func (t *table) Search(key []byte, maxVs *uint64) (entry *utils.Entry, err error) {
	idx := t.ss.Indexs()
	bloomFilter := utils.Filter(idx.BloomFilter)
	//先查询布隆过滤器
	if t.ss.HasBloomFilter() && !bloomFilter.MayContainKey(key) {
		return nil, utils.ErrKeyNotFound
	}
	iter := t.NewIterator(&utils.Options{})
	defer iter.Close()

	iter.Seek(key)
	if iter.Valid() {
		return nil, utils.ErrKeyNotFound
	}
	if utils.SameKey(key, iter.Item().Entry().Key) {
		if version := utils.ParseTs(iter.Item().Entry().Key); *maxVs < version {
			*maxVs = version
			return iter.Item().Entry(), nil
		}
	}
	return nil, utils.ErrKeyNotFound
}

type tableIterator struct {
	it       utils.Item
	opt      *utils.Options
	t        *table
	blockPos int
	bi       *blockIterator
	err      error
}

// Seek
// 二分法搜索 offsets
// 如果idx == 0 说明key只能在第一个block中 block[0].MinKey <= key
// 否则 block[0].MinKey > key
// 如果在 idx-1 的block中未找到key 那才可能在 idx 中
// 如果都没有，则当前key不再此table
func (itr *tableIterator) Seek(key []byte) {
	var ko pb.BlockOffset
	idx := sort.Search(len(itr.t.ss.Indexs().GetOffsets()), func(idx int) bool {
		utils.CondPanic(!itr.t.offsets(&ko, idx), fmt.Errorf("tableutils.Seek idx < 0 || idx > len(index.GetOffsets()"))
		return utils.CompareKeys(ko.GetKey(), key) > 0
	})
	if idx == 0 {
		itr.seekHelper(0, key)
		return
	}
	itr.seekHelper(idx-1, key)
	if itr.err == io.EOF {
		if idx == len(itr.t.ss.Indexs().Offsets) {
			return
		}
		itr.seekHelper(idx, key)
	}
}

func (it *tableIterator) Next() {
}
func (it *tableIterator) Valid() bool {
	return it == nil
}
func (it *tableIterator) Rewind() {
}
func (it *tableIterator) Item() utils.Item {
	return it.it
}
func (it *tableIterator) Close() error {
	return nil
}

func (t *table) NewIterator(options *utils.Options) utils.Iterator {
	return &tableIterator{
		opt: options,
		t:   t,
		bi:  &blockIterator{},
	}
}

//todo
func (it *tableIterator) seekHelper(blockIdx int, key []byte) {
	it.blockPos = blockIdx
	block, err := it.t.block(blockIdx)
	if err != nil {
		it.err = err
		return
	}
	it.bi.tableID = it.t.fid
	it.bi.blockID = it.blockPos
	it.bi.setBlock(block)
	it.bi.seek(key)
	it.err = it.bi.Error()
	it.it = it.bi.Item()
}

// 去加载sst对应的block
func (t *table) block(idx int) (*block, error) {
	utils.CondPanic(idx < 0, fmt.Errorf("idx=%d", idx))
	if idx >= len(t.ss.Indexs().Offsets) {
		return nil, errors.New("block out of index")
	}
	var b *block
	key := t.blockCacheKey(idx)
	blk, ok := t.lm.cache.blocks.Get(key)
	if ok && blk != nil {
		b, _ = blk.(*block)
		return b, nil
	}
	var ko pb.BlockOffset
	utils.CondPanic(!t.offsets(&ko, idx), fmt.Errorf("block t.offset id=%d", idx))
	b = &block{
		offset: int(ko.Offset),
	}
	var err error
	if b.data, err = t.read(b.offset, int(ko.GetLen())); err != nil {
		return nil, errors.Wrapf(err,
			"failed to read from sstable: %d at offset: %d, len: %d",
			t.ss.FID(), b.offset, ko.GetLen())
	}
	readPos := len(b.data) - 4 // First read checksum length.
	b.chkLen = int(utils.BytesToU32(b.data[readPos : readPos+4]))

	if b.chkLen > len(b.data) {
		return nil, errors.New("invalid checksum length. Either the data is " +
			"corrupted or the table options are incorrectly set")
	}

	readPos -= b.chkLen
	b.checksum = b.data[readPos : readPos+b.chkLen]

	readPos -= 4
	numEntries := int(utils.BytesToU32(b.data[readPos : readPos+4]))
	entriesIndexStart := readPos - (numEntries * 4)
	entriesIndexEnd := entriesIndexStart + numEntries*4

	b.entryOffsets = utils.BytesToU32Slice(b.data[entriesIndexStart:entriesIndexEnd])

	b.entriesIndexStart = entriesIndexStart

	b.data = b.data[:readPos+4]

	if err = b.verifyCheckSum(); err != nil {
		return nil, err
	}

	t.lm.cache.blocks.Set(key, b)

	return b, nil
}

func (t *table) read(off, sz int) ([]byte, error) {
	return t.ss.Bytes(off, sz)
}

// blockCacheKey is used to store blocks in the block cache.
func (t *table) blockCacheKey(idx int) []byte {
	utils.CondPanic(t.fid >= math.MaxUint32, fmt.Errorf("t.fid >= math.MaxUint32"))
	utils.CondPanic(uint32(idx) >= math.MaxUint32, fmt.Errorf("uint32(idx) >=  math.MaxUint32"))

	buf := make([]byte, 8)
	// Assume t.ID does not overflow uint32.
	binary.BigEndian.PutUint32(buf[:4], uint32(t.fid))
	binary.BigEndian.PutUint32(buf[4:], uint32(idx))
	return buf
}

func (t *table) offsets(ko *pb.BlockOffset, i int) bool {
	index := t.ss.Indexs()
	if i < 0 || i > len(index.GetOffsets()) {
		return false
	}
	*ko = *index.GetOffsets()[i]
	return true
}
