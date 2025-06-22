package data

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"hash/crc32"
	"sort"
	"testing"
)

func TestEncodeLogRecord(t *testing.T) {
	// 正常情况
	record1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-kv-go"),
		Type:  LogRecordNormal,
	}
	res1, n1 := EncodeLogRecord(record1)
	assert.NotNil(t, res1)
	assert.Greater(t, n1, int64(5))

	// value 为空情况
	record2 := &LogRecord{
		Key:  []byte("name"),
		Type: LogRecordNormal,
	}
	res2, n2 := EncodeLogRecord(record2)
	assert.NotNil(t, res2)
	assert.Greater(t, n2, int64(5))

	// 类型为 deleted
	record3 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-kv-go"),
		Type:  LogRecordDeleted,
	}
	res3, n3 := EncodeLogRecord(record3)
	assert.NotNil(t, res3)
	assert.Greater(t, n3, int64(5))
}

func TestDecodeLogRecordHeader(t *testing.T) {
	// 正常情况
	headerBuf1 := []byte{81, 61, 93, 186, 0, 8, 26}
	h1, size1 := decodeLogRecordHeader(headerBuf1)
	assert.NotNil(t, h1)
	assert.Equal(t, int64(7), size1)
	assert.Equal(t, uint32(3126672721), h1.crc)
	assert.Equal(t, LogRecordNormal, h1.recordType)
	assert.Equal(t, uint32(4), h1.keySize)
	assert.Equal(t, uint32(13), h1.valueSize)

	// value 为空的情况
	headerBuf2 := []byte{9, 252, 88, 14, 0, 8, 0}
	h2, size2 := decodeLogRecordHeader(headerBuf2)
	assert.NotNil(t, h2)
	assert.Equal(t, int64(7), size2)
	assert.Equal(t, uint32(240712713), h2.crc)
	assert.Equal(t, LogRecordNormal, h2.recordType)
	assert.Equal(t, uint32(4), h2.keySize)
	assert.Equal(t, uint32(0), h2.valueSize)

	// 类型为 deleted
	headerBuf3 := []byte{23, 6, 58, 223, 1, 8, 26}
	h3, size3 := decodeLogRecordHeader(headerBuf3)

	assert.NotNil(t, h3)
	assert.Equal(t, int64(7), size3)
	assert.Equal(t, uint32(3745121815), h3.crc)
	assert.Equal(t, LogRecordDeleted, h3.recordType)
	assert.Equal(t, uint32(4), h3.keySize)
	assert.Equal(t, uint32(13), h3.valueSize)
}

func TestGetLogRecordCrc(t *testing.T) {
	record1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-kv-go"),
		Type:  LogRecordNormal,
	}
	headerBuf1 := []byte{81, 61, 93, 186, 0, 8, 26}
	crc1 := getLogRecordCRC(record1, headerBuf1[crc32.Size:])
	assert.Equal(t, uint32(3126672721), crc1)

	record2 := &LogRecord{
		Key:  []byte("name"),
		Type: LogRecordNormal,
	}
	headerBuf2 := []byte{9, 252, 88, 14, 0, 8, 0}
	crc2 := getLogRecordCRC(record2, headerBuf2[crc32.Size:])
	assert.Equal(t, uint32(240712713), crc2)

	record3 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-kv-go"),
		Type:  LogRecordDeleted,
	}
	headerBuf3 := []byte{23, 6, 58, 223, 1, 8, 26}
	crc3 := getLogRecordCRC(record3, headerBuf3[crc32.Size:])
	assert.Equal(t, uint32(3745121815), crc3)
}

// 给你一个整数数组 nums ，判断是否存在三元组 [nums[i], nums[j], nums[k]]
// 满足 i != j、i != k 且 j != k ，同时还满足 nums[i] + nums[j] + nums[k] == 0 。请
//你返回所有和为 0 且不重复的三元组。
//
//输入：nums = [-1,0,1,2,-1,-4]
//输出：[[-1,-1,2],[-1,0,1]]

func getThreeSum(nums []int) [][]int {
	if len(nums) < 3 {
		return nil
	}

	sort.Ints(nums)

	var res [][]int
	for i := 0; i < len(nums); i++ {
		if i > 0 && nums[i] == nums[i-1] {
			continue // 跳过重复的元素
		}

		left, right := i+1, len(nums)-1
		for left < right {
			sum := nums[i] + nums[left] + nums[right]
			if sum == 0 {
				res = append(res, []int{nums[i], nums[left], nums[right]})
				left++
				right--
				for left < right && nums[left] == nums[left-1] {
					left++ // 跳过重复的元素
				}
				for left < right && nums[right] == nums[right+1] {
					right-- // 跳过重复的元素
				}
			} else if sum < 0 {
				left++
			} else {
				right--
			}
		}
	}

	return res
}

func TestThreeNums(t *testing.T) {
	nums := []int{-1, 0, 1, 2, -1, -4}

	res := getThreeSum(nums)
	fmt.Println(res)
}
