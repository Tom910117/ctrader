package ctrader

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"golang.org/x/exp/slog"
	"google.golang.org/protobuf/proto"

	"github.com/diegobernardes/ctrader/openapi"
)

// raceFakeTransport 模擬底層 TCP：收到請求後會在背景非同步回傳封包，用來與斷線清理互相競速
type raceFakeTransport struct {
	mu         sync.RWMutex
	handlerMsg func([]byte)
	handlerErr func(error)
	closed     bool
	replyWg    sync.WaitGroup
}

func (f *raceFakeTransport) start(string) error { return nil }

func (f *raceFakeTransport) stop() error {
	f.mu.Lock()
	f.closed = true
	f.mu.Unlock()
	// 模擬真實 transportTCP.stop() 等待所有收訊回呼結束
	f.replyWg.Wait()
	return nil
}

func (f *raceFakeTransport) setHandler(msg func([]byte), err func(error)) {
	f.mu.Lock()
	f.handlerMsg = msg
	f.handlerErr = err
	f.mu.Unlock()
}

func (f *raceFakeTransport) send(payload []byte) error {
	f.mu.RLock()
	closed := f.closed
	cb := f.handlerMsg
	f.mu.RUnlock()

	if closed {
		return errors.New("transport closed")
	}

	var reqMsg openapi.ProtoMessage
	if err := proto.Unmarshal(payload, &reqMsg); err != nil {
		return err
	}

	// 背景非同步回覆，刻意製造 handlerMessage 與 abortPendingRequests / ctx 超時的併發競速
	f.replyWg.Add(1)
	go func() {
		defer f.replyWg.Done()
		time.Sleep(time.Millisecond)

		respType := uint32(openapi.ProtoPayloadType_HEARTBEAT_EVENT)
		resp := &openapi.ProtoMessage{
			ClientMsgId: reqMsg.ClientMsgId,
			PayloadType: &respType,
			Payload:     []byte{},
		}
		raw, err := proto.Marshal(resp)
		if err == nil && cb != nil {
			cb(raw)
		}
	}()
	return nil
}

// 1. 驗證修正後：從持有 c.wg 的協程呼叫 handlerError 不會再死鎖，必須立刻返回
func TestHandlerError_NoSelfWaitDeadlock(t *testing.T) {
	c := &Client{
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		reconnectCh:     make(chan struct{}, 1),
		requestRegistry: make(map[string]chan *openapi.ProtoMessage),
	}

	c.wg.Add(1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer c.wg.Done()
		// 模擬多次併發報錯，驗證非阻塞信箱不會卡死
		c.handlerError(errors.New("network failure 1"))
		c.handlerError(errors.New("network failure 2"))
	}()

	select {
	case <-done:
		// 順利退場，代表死鎖已解除
	case <-time.After(2 * time.Second):
		t.Fatal("handlerError() blocked — deadlock still exists")
	}
	c.wg.Wait()
}

//  2. 高併發轟炸測試：50 條協程狂發 sendRequest，同時觸發 handlerMessage、超時與斷線清理
//     驗證：零 Data Race、零 concurrent map writes、零 send on closed channel Panic
func TestClient_ConcurrentRequestsAndAbort_Race(t *testing.T) {
	fake := &raceFakeTransport{}
	c := &Client{
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		HandlerEvent:    func(proto.Message) {},
		transport:       fake,
		reconnectCh:     make(chan struct{}, 1),
		requestRegistry: make(map[string]chan *openapi.ProtoMessage),
	}
	fake.setHandler(c.handlerMessage, c.handlerError)

	const workers = 50
	var reqWg sync.WaitGroup
	reqWg.Add(workers)

	for i := 0; i < workers; i++ {
		go func(idx int) {
			defer reqWg.Done()
			// 一半給極短超時刻意撞機房回覆，一半正常等回覆或被 abortPendingRequests 叫醒
			timeout := 50 * time.Millisecond
			if idx%2 == 0 {
				timeout = time.Millisecond
			}
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()

			req := &openapi.ProtoOAApplicationAuthReq{}
			_, _ = c.sendRequest(ctx, req)
		}(i)
	}

	// 在請求狂飛的中途，故意觸發強制關閉在途通道與清理連線
	time.Sleep(time.Millisecond)
	c.abortPendingRequests()
	c.disconnectSession()

	reqWg.Wait()
}

// 3. 驗證 supervisor 與 Stop() 能安全關機不卡死
func TestClient_StopSupervisorCleanly(t *testing.T) {
	c := &Client{
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		reconnectCh:     make(chan struct{}, 1),
		requestRegistry: make(map[string]chan *openapi.ProtoMessage),
	}
	c.closeCtx, c.closeCancel = context.WithCancel(context.Background())

	c.supWg.Add(1)
	go c.supervisor()

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = c.Stop()
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop() timed out waiting for supervisor")
	}
}
