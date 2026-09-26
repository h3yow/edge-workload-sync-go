package edgesync

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"sort"
	"sync"
)

// Store 是基于 JSON 文件的持久化键值存储，用于模拟云端与边缘两端的
// 本地持久化。每次写入整体落盘，保证崩溃后状态可恢复。
type Store struct {
	mu   sync.Mutex
	path string
	data map[string]string // key -> base64 编码的值
}

// OpenStore 打开（或创建）指定路径的持久化存储。
func OpenStore(path string) (*Store, error) {
	s := &Store{path: path, data: map[string]string{}}
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return s, nil
		}
		return nil, err
	}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &s.data); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// Get 读取键对应的值，不存在时返回 false。
func (s *Store) Get(key string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	enc, ok := s.data[key]
	if !ok {
		return nil, false
	}
	raw, err := base64.StdEncoding.DecodeString(enc)
	if err != nil {
		return nil, false
	}
	return raw, true
}

// Put 写入键值并立即落盘。
func (s *Store) Put(key string, val []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = base64.StdEncoding.EncodeToString(val)
	return s.flushLocked()
}

// Delete 删除键并落盘。
func (s *Store) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
	return s.flushLocked()
}

// Keys 返回排序后的所有键（用于测试与调试）。
func (s *Store) Keys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func (s *Store) flushLocked() error {
	raw, err := json.MarshalIndent(s.data, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}
