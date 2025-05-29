package fio

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// TestHelper 用于测试的辅助结构
type TestHelper struct {
	path string
	fio  *FileIO
}

// setupTest 创建测试环境
func setupTest(t *testing.T) *TestHelper {
	path := filepath.Join(os.TempDir(), "test.data")
	fio, err := NewFileIOManager(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)
	return &TestHelper{path: path, fio: fio}
}

// tearDown 清理测试环境
func (h *TestHelper) tearDown() {
	if h.fio != nil {
		_ = h.fio.Close()
	}
	_ = os.RemoveAll(h.path)
}

func destroyFile(name string) {
	if err := os.RemoveAll(name); err != nil {
		panic(err)
	}
}

func TestNewFileIOManager(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		wantErr bool
	}{
		{
			name:    "valid path",
			path:    filepath.Join(os.TempDir(), "data.data"),
			wantErr: false,
		},
		{
			name:    "invalid path",
			path:    filepath.Join("/nonexistent", "data.data"),
			wantErr: true,
		},
		{
			name:    "empty path",
			path:    "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fio, err := NewFileIOManager(tt.path)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, fio)
			} else {
				assert.Nil(t, err)
				assert.NotNil(t, fio)
				defer destroyFile(tt.path)
			}
		})
	}
}

func TestFileIO_Write(t *testing.T) {
	h := setupTest(t)
	defer h.tearDown()

	tests := []struct {
		name    string
		data    []byte
		wantN   int
		wantErr bool
	}{
		{
			name:    "empty write",
			data:    []byte(""),
			wantN:   0,
			wantErr: false,
		},
		{
			name:    "small write",
			data:    []byte("hello"),
			wantN:   5,
			wantErr: false,
		},
		{
			name:    "large write",
			data:    make([]byte, 1024*1024), // 1MB
			wantN:   1024 * 1024,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n, err := h.fio.Write(tt.data)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.Nil(t, err)
				assert.Equal(t, tt.wantN, n)
			}
		})
	}
}

func TestFileIO_Read(t *testing.T) {
	h := setupTest(t)
	defer h.tearDown()

	// 写入测试数据
	testData := []byte("Hello, World!")
	n, err := h.fio.Write(testData)
	assert.Nil(t, err)
	assert.Equal(t, len(testData), n)

	tests := []struct {
		name    string
		offset  int64
		len     int
		want    []byte
		wantErr bool
	}{
		{
			name:    "read full",
			offset:  0,
			len:     len(testData),
			want:    testData,
			wantErr: false,
		},
		{
			name:    "read partial",
			offset:  0,
			len:     5,
			want:    []byte("Hello"),
			wantErr: false,
		},
		{
			name:    "read with offset",
			offset:  7,
			len:     5,
			want:    []byte("World"),
			wantErr: false,
		},
		{
			name:    "read beyond EOF",
			offset:  100,
			len:     5,
			want:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := make([]byte, tt.len)
			n, err := h.fio.Read(buf, tt.offset)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.Nil(t, err)
				assert.Equal(t, len(tt.want), n)
				assert.Equal(t, tt.want, buf[:n])
			}
		})
	}
}

func TestFileIO_Concurrent(t *testing.T) {
	h := setupTest(t)
	defer h.tearDown()

	var wg sync.WaitGroup
	concurrency := 10
	iterations := 100

	// 并发写入
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				data := []byte("test")
				n, err := h.fio.Write(data)
				assert.Nil(t, err)
				assert.Equal(t, len(data), n)
			}
		}(i)
	}
	wg.Wait()

	// 验证文件大小
	info, err := os.Stat(h.path)
	assert.Nil(t, err)
	assert.Equal(t, int64(4*concurrency*iterations), info.Size())
}

func TestFileIO_Sync(t *testing.T) {
	h := setupTest(t)
	defer h.tearDown()

	// 写入一些数据
	_, err := h.fio.Write([]byte("test data"))
	assert.Nil(t, err)

	// 测试同步
	err = h.fio.Sync()
	assert.Nil(t, err)

	// 关闭文件后测试同步
	err = h.fio.Close()
	assert.Nil(t, err)
	err = h.fio.Sync()
	assert.Error(t, err)
}

func TestFileIO_Close(t *testing.T) {
	h := setupTest(t)
	defer h.tearDown()

	// 测试正常关闭
	err := h.fio.Close()
	assert.Nil(t, err)

	// 测试重复关闭
	err = h.fio.Close()
	assert.Error(t, err)

	// 测试关闭后的操作
	_, err = h.fio.Write([]byte("test"))
	assert.Error(t, err)
}

func TestFileIO_ReadWrite_EdgeCases(t *testing.T) {
	h := setupTest(t)
	defer h.tearDown()

	// 测试大数据块
	largeData := make([]byte, 10*1024*1024) // 10MB
	n, err := h.fio.Write(largeData)
	assert.Nil(t, err)
	assert.Equal(t, len(largeData), n)

	// 测试读取大数据块
	readBuf := make([]byte, 10*1024*1024)
	n, err = h.fio.Read(readBuf, 0)
	assert.Nil(t, err)
	assert.Equal(t, len(largeData), n)

	// 测试零字节读写
	n, err = h.fio.Write([]byte{})
	assert.Nil(t, err)
	assert.Equal(t, 0, n)

	emptyBuf := make([]byte, 0)
	n, err = h.fio.Read(emptyBuf, 0)
	assert.Nil(t, err)
	assert.Equal(t, 0, n)
}
