package edgesync

import (
	"errors"
	"sync"
)

// ErrLinkDown 表示链路断开，变更无法投递。
var ErrLinkDown = errors.New("link is down")

// Link 模拟云端与边缘之间的链路，可断连；上行投递失败时由边缘暂存。
type Link struct {
	mu        sync.Mutex
	cloud     *Cloud
	connected bool
}

// NewLink 创建链路，初始为连接状态。
func NewLink(cloud *Cloud) *Link {
	return &Link{cloud: cloud, connected: true}
}

// Connect 恢复连接。
func (l *Link) Connect() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.connected = true
}

// Disconnect 断开连接。
func (l *Link) Disconnect() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.connected = false
}

// Connected 返回当前连接状态。
func (l *Link) Connected() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.connected
}

// Deliver 向云端投递一条边缘变更；断连时返回 ErrLinkDown，由调用方暂存。
func (l *Link) Deliver(ch LocalChange) error {
	_, err := l.DeliverResult(ch)
	return err
}

// DeliverResult 与 Deliver 相同，但透传云端的结构化判定结果，
// 使重放方能区分每条变更的结局（生效/重复/拒绝）。
func (l *Link) DeliverResult(ch LocalChange) (ReceiveResult, error) {
	if !l.Connected() {
		return ReceiveResult{}, ErrLinkDown
	}
	return l.cloud.ReceiveChangeResult(ch)
}
