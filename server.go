package simple_rpc

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"reflect"
	"simple_rpc/codec"
	"strings"
	"sync"
	"time"
)

const MagicNumber = 0x3bef5c

// Option 消息的编解码方式
// 一般来说，涉及协议协商的这部分信息，需要设计固定的字节来传输的。
// 但是为了实现上更简单，Simple RPC 客户端固定采用 JSON 编码 Option，
// 后续的 header 和 body 的编码方式由 Option 中的 CodeType 指定，
// 服务端首先使用 JSON 解码 Option，然后通过 Option 的 CodeType 解码剩余的内容。
// 即报文将以这样的形式发送：
//
//	Option{MagicNumber: xxx, CodecType: xxx} | Header{ServiceMethod ...} | Body interface{} |
//
// | <------      固定 JSON 编码      ------>  | <-------   编码方式由 CodeType 决定   ------->|
// 在一次连接中，Option 固定在报文的最开始，Header 和 Body 可以有多个，即报文可能是这样的。
// | Option | Header1 | Body1 | Header2 | Body2 | ...
type Option struct {
	MagicNumber    int           // MagicNumber marks this a simple rpc request
	CodecType      codec.Type    // client may choose different Codec to encode body
	ConnectTimeout time.Duration // 0 means no limit
	HandleTimeout  time.Duration
}

// DefaultOption 将超时设定放在了 Option 中。ConnectTimeout 默认值为 10s，HandleTimeout 默认值为 0，即不设限。
var DefaultOption = &Option{
	MagicNumber:    MagicNumber,
	CodecType:      codec.GobType,
	ConnectTimeout: time.Second * 10,
}

// Server represents an RPC Server.
// 通过反射结构体已经映射为服务，但请求的处理过程还没有完成。从接收到请求到回复还差以下几个步骤：
// 第一步，根据入参类型，将请求的 body 反序列化；
// 第二步，调用 service.call，完成方法调用；
// 第三步，将 reply 序列化为字节流，构造响应报文，返回。
type Server struct {
	serviceMap sync.Map
}

// NewServer returns a new Server.
func NewServer() *Server {
	return &Server{}
}

// DefaultServer is the default instance of *Server.
// DefaultServer 是一个默认的 Server 实例，主要为了用户使用方便。
var DefaultServer = NewServer()

// ServeConn runs the server on a single connection.
// ServeConn blocks, serving the connection until the client hangs up.
// ServeConn 的实现就和之前讨论的通信过程紧密相关了
// 首先使用 json.NewDecoder 反序列化得到 Option 实例，检查 MagicNumber 和 CodeType 的值是否正确。
// 然后根据 CodeType 得到对应的消息编解码器，接下来的处理交给 serverCodec。
func (server *Server) ServeConn(conn io.ReadWriteCloser) {
	defer func() { _ = conn.Close() }()
	var opt Option
	if err := json.NewDecoder(conn).Decode(&opt); err != nil {
		log.Println("rpc server: options error: ", err)
		return
	}
	if opt.MagicNumber != MagicNumber {
		log.Printf("rpc server: invalid magic number %x", opt.MagicNumber)
		return
	}
	f := codec.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		log.Printf("rpc server: invalid codec type %s", opt.CodecType)
		return
	}
	server.serveCodec(f(conn), &opt)
}

// invalidRequest is a placeholder for response argv when error occurs
var invalidRequest = struct{}{}

// serveCodec 的过程非常简单。主要包含三个阶段
// 读取请求 readRequest
// 处理请求 handleRequest
// 回复请求 sendResponse
// 在一次连接中，允许接收多个请求，即多个 request header 和 request body，因此这里使用了 for 无限制地等待请求的到来，直到发生错误（例如连接被关闭，接收到的报文有问题等），这里需要注意的点有三个：
// handleRequest 使用了协程并发执行请求。
// 处理请求是并发的，但是回复请求的报文必须是逐个发送的，并发容易导致多个回复报文交织在一起，客户端无法解析。在这里使用锁(sending)保证。
// 尽力而为，只有在 header 解析失败时，才终止循环。
func (server *Server) serveCodec(cc codec.Codec, opt *Option) {
	sending := new(sync.Mutex) // make sure to send a complete response
	wg := new(sync.WaitGroup)  // wait until all request are handled
	for {
		req, err := server.readRequest(cc)
		if err != nil {
			if req == nil {
				break // it's not possible to recover, so close the connection
			}
			req.h.Error = err.Error()
			server.sendResponse(cc, req.h, invalidRequest, sending)
			continue
		}
		wg.Add(1)
		go server.handleRequest(cc, req, sending, wg, opt.HandleTimeout)
	}
	wg.Wait()
	_ = cc.Close()
}

// findService 方法，即通过 ServiceMethod 从 serviceMap 中找到对应的 service
// findService 的实现看似比较繁琐，但是逻辑还是非常清晰的。因为 ServiceMethod 的构成是 “Service.Method”，因此先将其分割成 2 部分，
// 第一部分是 Service 的名称，第二部分即方法名。
// 先在 serviceMap 中找到对应的 service 实例，再从 service 实例的 method 中，找到对应的 methodType。
func (server *Server) findService(serviceMethod string) (svc *service, mType *methodType, err error) {
	dot := strings.LastIndex(serviceMethod, ".")
	if dot < 0 {
		err = errors.New("rpc server: service/method request ill-formed: " + serviceMethod)
		return
	}
	serviceName, methodName := serviceMethod[:dot], serviceMethod[dot+1:]
	sci, ok := server.serviceMap.Load(serviceName)
	if !ok {
		err = errors.New("rpc server: can't find service " + serviceName)
		return
	}
	svc = sci.(*service)
	mType = svc.method[methodName]
	if mType == nil {
		err = errors.New("rpc server: can't find method " + methodName)
	}
	return
}

// request stores all information of a call
type request struct {
	h            *codec.Header // header of request
	argV, replyV reflect.Value // argv and reply of request
	mType        *methodType
	svc          *service
}

func (server *Server) readRequestHeader(cc codec.Codec) (*codec.Header, error) {
	var h codec.Header
	if err := cc.ReadHeader(&h); err != nil {
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			log.Println("rpc server: read header error:", err)
		}
		return nil, err
	}
	return &h, nil
}

// readRequest 方法中最重要的部分，即通过 newArgV() 和 newReplyV() 两个方法创建出两个入参实例，
// 然后通过 cc.ReadBody() 将请求报文反序列化为第一个入参 argV，
// 在这里同样需要注意 argV 可能是值类型，也可能是指针类型，所以处理方式有点差异。
func (server *Server) readRequest(cc codec.Codec) (*request, error) {
	h, err := server.readRequestHeader(cc)
	if err != nil {
		return nil, err
	}
	req := &request{h: h}
	req.svc, req.mType, err = server.findService(h.ServiceMethod)
	if err != nil {
		return req, err
	}
	req.argV = req.mType.newArgV()
	req.replyV = req.mType.newReplyV()

	// make sure that argVi is a pointer, ReadBody need a pointer as parameter
	argVI := req.argV.Interface()
	if req.argV.Type().Kind() != reflect.Ptr {
		argVI = req.argV.Addr().Interface()
	}
	if err = cc.ReadBody(argVI); err != nil {
		log.Println("rpc server: read body err:", err)
		return req, err
	}
	return req, nil
}

func (server *Server) sendResponse(cc codec.Codec, h *codec.Header, body interface{}, sending *sync.Mutex) {
	sending.Lock()
	defer sending.Unlock()
	if err := cc.Write(h, body); err != nil {
		log.Println("rpc server: write response error:", err)
	}
}

// handleRequest 的实现非常简单，通过 req.svc.call 完成方法调用，将 replyV 传递给 sendResponse 完成序列化即可。
// 需要确保 sendResponse 仅调用一次，因此将整个过程拆分为 called 和 sent 两个阶段，在这段代码中只会发生如下两种情况：
// called 信道接收到消息，代表处理没有超时，继续执行 sendResponse。
// time.After() 先于 called 接收到消息，说明处理已经超时，called 和 sent 都将被阻塞。
// 在 case <-time.After(timeout) 处调用 sendResponse。
func (server *Server) handleRequest(cc codec.Codec, req *request, sending *sync.Mutex, wg *sync.WaitGroup, timeout time.Duration) {
	defer wg.Done()
	called := make(chan struct{})
	sent := make(chan struct{})
	go func() {
		err := req.svc.call(req.mType, req.argV, req.replyV)
		called <- struct{}{}
		if err != nil {
			req.h.Error = err.Error()
			server.sendResponse(cc, req.h, invalidRequest, sending)
			sent <- struct{}{}
			return
		}
		server.sendResponse(cc, req.h, req.replyV.Interface(), sending)
		sent <- struct{}{}
	}()

	if timeout == 0 {
		<-called
		<-sent
		return
	}
	select {
	case <-time.After(timeout):
		req.h.Error = fmt.Sprintf("rpc server: request handle timeout: expect within %s", timeout)
		server.sendResponse(cc, req.h, invalidRequest, sending)
	case <-called:
		<-sent
	}
}

// Accept accepts connections on the listener and serves requests
// for each incoming connection.
// 实现了 Accept 方式，net.Listener 作为参数，for 循环等待 socket 连接建立，并开启子协程处理，处理过程交给了 ServerConn 方法。
func (server *Server) Accept(lis net.Listener) {
	for {
		conn, err := lis.Accept()
		if err != nil {
			log.Println("rpc server: accept error:", err)
			return
		}
		go server.ServeConn(conn)
	}
}

// Accept accepts connections on the listener and serves requests
// for each incoming connection.
func Accept(lis net.Listener) { DefaultServer.Accept(lis) }

// Register publishes in the server the set of methods of the
// receiver value that satisfy the following conditions:
//   - exported method of exported type
//   - two arguments, both of exported type
//   - the second argument is a pointer
//   - one return value, of type error
func (server *Server) Register(rcv interface{}) error {
	s := newService(rcv)
	if _, dup := server.serviceMap.LoadOrStore(s.name, s); dup {
		return errors.New("rpc: service already defined: " + s.name)
	}
	return nil
}

// Register publishes the receiver's methods in the DefaultServer.
func Register(rcv interface{}) error { return DefaultServer.Register(rcv) }
