package ctrader

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/satori/uuid"
	"golang.org/x/exp/slog"
	"google.golang.org/protobuf/proto"

	"github.com/diegobernardes/ctrader/openapi"
)

type clientTransport interface {
	start(string) error
	stop() error
	send([]byte) error
	setHandler(func([]byte), func(error))
}

type Client struct {
	ApplicationClientID string
	ApplicationSecret   string
	AccountID           string // 記住交易帳號 ID，供開機與斷線重連時自動授權
	AccessToken         string // 記住 AccessToken，供開機與斷線重連時自動授權
	HandlerEvent        func(proto.Message)
	Deadline            time.Duration
	Logger              *slog.Logger
	Live                bool

	accountID int64

	// (e) 保護 transport 介面熱替換與併發讀取的讀寫鎖
	transportMutex sync.RWMutex
	transport      clientTransport

	stopSignal atomic.Bool
	wg         sync.WaitGroup

	// 監督者模式 (Supervisor) 控制欄位
	reconnectCh chan struct{}
	closeCtx    context.Context
	closeCancel context.CancelFunc
	supWg       sync.WaitGroup

	requestRegistry      map[string]chan *openapi.ProtoMessage
	requestRegistryMutex sync.Mutex
}

// Start 初始化資源、建立首次連線與雙重授權，並啟動常駐監督協程
func (c *Client) Start() error {
	if c.AccountID != "" {
		parsedID, err := strconv.ParseInt(c.AccountID, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid CTRADER_ACCOUNT_ID '%s': %w", c.AccountID, err)
		}
		c.accountID = parsedID
	}

	c.requestRegistry = make(map[string]chan *openapi.ProtoMessage)
	c.reconnectCh = make(chan struct{}, 1)
	c.closeCtx, c.closeCancel = context.WithCancel(context.Background())

	if err := c.connectSession(); err != nil {
		return err
	}

	c.supWg.Add(1)
	go c.supervisor()

	return nil
}

// Stop 由外部主程式關機時呼叫，先終止監督協程，再關閉連線與清理由通道
func (c *Client) Stop() error {
	if c.closeCancel != nil {
		c.closeCancel()
	}
	c.supWg.Wait()

	c.disconnectSession()
	c.abortPendingRequests()
	return nil
}

// supervisor 獨立常駐的監督協程，專門負責接收斷線通報並執行清理與重連
func (c *Client) supervisor() {
	defer c.supWg.Done()

	for {
		select {
		case <-c.closeCtx.Done():
			return

		case <-c.reconnectCh:
			c.Logger.Warn("🛠️ [監督協程] 收到斷線通知，開始清理舊連線與協程...")

			// 1. 等待舊的 keepalive 與 receive 協程完全退出
			c.disconnectSession()

			// 2. 關閉所有卡在等待回應的請求通道，讓它們立刻返回錯誤
			c.abortPendingRequests()

			// 3. 抽乾在 disconnectSession 期間可能多塞進來的重複斷線通知
			select {
			case <-c.reconnectCh:
			default:
			}

			// 4. 進入重連迴圈，直到成功或收到關機指令
			for {
				if c.closeCtx.Err() != nil {
					return
				}

				c.Logger.Info("🔄 [監督協程] 正在嘗試重新連線至 cTrader 機房...")
				if err := c.connectSession(); err != nil {
					c.Logger.Error("❌ [監督協程] 重連失敗，2 秒後重試", "error", err.Error())

					// 使用 select 代替死板的 time.Sleep，確保重連等待期間按下 Ctrl+C 也能 0 秒關機
					select {
					case <-c.closeCtx.Done():
						return
					case <-time.After(2 * time.Second):
					}
					continue
				}

				c.Logger.Info("✅ [監督協程] cTrader 斷線重連與雙重授權成功！恢復運作。")
				break
			}
		}
	}
}

// connectSession 負責建立單次 TCP 連線、替換 transport、執行 App 與 Account 授權並啟動 keepalive
func (c *Client) connectSession() error {
	c.stopSignal.Store(false)

	newTransport := &transportTCP{deadline: c.Deadline}
	var address string
	if c.Live {
		address = "live.ctraderapi.com:5035"
	} else {
		address = "demo.ctraderapi.com:5035"
	}
	newTransport.setHandler(c.handlerMessage, c.handlerError)

	if err := newTransport.start(address); err != nil {
		return fmt.Errorf("failed to open the transport: %w", err)
	}

	c.transportMutex.Lock()
	c.transport = newTransport
	c.transportMutex.Unlock()

	ctx, ctxCancel := context.WithTimeout(c.closeCtx, 5*time.Second)
	defer ctxCancel()

	if err := c.applicationAuthorization(ctx); err != nil {
		c.disconnectSession()
		return fmt.Errorf("failed to authenticate the application: %w", err)
	}

	if c.accountID != 0 && c.AccessToken != "" {
		if err := c.accountAuthorization(ctx); err != nil {
			c.disconnectSession()
			return fmt.Errorf("failed to authenticate the trading account: %w", err)
		}
	}

	c.keepalive()
	return nil
}

// disconnectSession 負責安全停止當前連線的 keepalive 與 receive 協程
func (c *Client) disconnectSession() {
	c.transportMutex.Lock()
	tp := c.transport
	c.transport = nil
	c.transportMutex.Unlock()

	c.stopSignal.Store(true)
	c.wg.Wait()

	if tp != nil {
		if err := tp.stop(); err != nil {
			c.Logger.Warn("failed to close the transport", "error", err.Error())
		}
	}
}

// abortPendingRequests 強制關閉所有未完成的在途請求通道，防止協程永久卡死
func (c *Client) abortPendingRequests() {
	c.requestRegistryMutex.Lock()
	defer c.requestRegistryMutex.Unlock()

	for id, ch := range c.requestRegistry {
		close(ch)
		delete(c.requestRegistry, id)
	}
}

func (c *Client) handlerMessage(payload []byte) {
	var msg openapi.ProtoMessage
	if err := proto.Unmarshal(payload, &msg); err != nil {
		c.Logger.Error("failed to unmarshal message", "error", err)
		return
	}
	if msg.GetClientMsgId() == "" {
		message, err := mappingResponse(msg.GetPayloadType())
		if err != nil {
			c.Logger.Error("unknow message type", "error", err)
			return
		}
		if err = proto.Unmarshal(msg.GetPayload(), message); err != nil {
			c.Logger.Error("failed to unmarshal payload", "error", err)
			return
		}
		c.HandlerEvent(message)
	} else {
		// 在鎖的保護下完成寫入並立刻從註冊表移除，徹底杜絕 send on closed channel Panic
		c.requestRegistryMutex.Lock()
		chanResponse, ok := c.requestRegistry[msg.GetClientMsgId()]
		if ok {
			chanResponse <- &msg
			delete(c.requestRegistry, msg.GetClientMsgId())
		}
		c.requestRegistryMutex.Unlock()

		if !ok {
			c.Logger.Error("client message ID not found", "clientMessageID", msg.GetClientMsgId())
			return
		}
	}
}

// handlerError 僅負責非阻塞地將斷線訊號投入信箱，不阻塞呼叫者
func (c *Client) handlerError(err error) {
	c.Logger.Error("Asynchronous error", "error", err.Error())
	select {
	case c.reconnectCh <- struct{}{}:
	default:
	}
}

func (c *Client) sendRequest(ctx context.Context, req proto.Message) (proto.Message, error) {
	payloadType, err := mappingPayloadType(req)
	if err != nil {
		return nil, fmt.Errorf("failed to get the payload type: %w", err)
	}

	payloadBase, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal base request: %w", err)
	}

	id := uuid.NewV4().String()
	reqType := uint32(payloadType)
	message := openapi.ProtoMessage{
		ClientMsgId: &id,
		Payload:     payloadBase,
		PayloadType: &reqType,
	}
	payload, err := proto.Marshal(&message)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	chanResponse := make(chan *openapi.ProtoMessage, 1)
	c.requestRegistryMutex.Lock()
	c.requestRegistry[id] = chanResponse
	c.requestRegistryMutex.Unlock()
	defer func() {
		c.requestRegistryMutex.Lock()
		delete(c.requestRegistry, id)
		c.requestRegistryMutex.Unlock()
	}()

	c.transportMutex.RLock()
	tp := c.transport
	c.transportMutex.RUnlock()

	if tp == nil {
		return nil, fmt.Errorf("transport is not initialized")
	}

	if errSend := tp.send(payload); errSend != nil {
		return nil, fmt.Errorf("failed to send the message: %w", errSend)
	}

	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("context error: %w", ctx.Err())
	case messageBase, ok := <-chanResponse:
		if !ok || messageBase == nil {
			return nil, fmt.Errorf("connection closed while waiting for response")
		}
		message, errMessage := mappingResponse(messageBase.GetPayloadType())
		if errMessage != nil {
			return nil, fmt.Errorf("failed to get the response type: %w", errMessage)
		}
		if err = proto.Unmarshal(messageBase.GetPayload(), message); err != nil {
			return nil, fmt.Errorf("failed to unmarshal the response: %w", err)
		}
		return message, nil
	}
}

func (c *Client) sendEvent(ctx context.Context, e proto.Message) error {
	payload, err := proto.Marshal(e)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}
	if err = ctx.Err(); err != nil {
		return fmt.Errorf("context error: %w", err)
	}

	c.transportMutex.RLock()
	tp := c.transport
	c.transportMutex.RUnlock()

	if tp == nil {
		return fmt.Errorf("transport is not initialized")
	}

	if errSend := tp.send(payload); errSend != nil {
		return fmt.Errorf("failed to send the message: %w", errSend)
	}
	return nil
}

func (c *Client) applicationAuthorization(ctx context.Context) error {
	req := &openapi.ProtoOAApplicationAuthReq{
		ClientId:     &c.ApplicationClientID,
		ClientSecret: &c.ApplicationSecret,
	}
	_, err := Command[*openapi.ProtoOAApplicationAuthReq, *openapi.ProtoOAApplicationAuthRes](ctx, c, req)
	if err != nil {
		return fmt.Errorf("failed to send the message: %w", err)
	}
	return nil
}

func (c *Client) accountAuthorization(ctx context.Context) error {
	req := &openapi.ProtoOAAccountAuthReq{
		CtidTraderAccountId: &c.accountID,
		AccessToken:         &c.AccessToken,
	}
	_, err := Command[*openapi.ProtoOAAccountAuthReq, *openapi.ProtoOAAccountAuthRes](ctx, c, req)
	if err != nil {
		return fmt.Errorf("failed to authorize trading account: %w", err)
	}
	return nil
}

func (c *Client) keepalive() {
	c.wg.Add(1)
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer func() {
			ticker.Stop()
			c.wg.Done()
		}()
		payloadTypeRaw := openapi.ProtoPayloadType_HEARTBEAT_EVENT
		payloadType := uint32(payloadTypeRaw)
		req := openapi.ProtoMessage{
			PayloadType: &payloadType,
		}
		for range ticker.C {
			if c.stopSignal.Load() {
				return
			}
			if err := c.sendEvent(context.Background(), &req); err != nil {
				c.handlerError(fmt.Errorf("failed to send the heartbeat event: %w", err))
				return
			}
		}
	}()
}
