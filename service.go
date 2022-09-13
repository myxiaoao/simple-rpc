package simple_rpc

import (
	"go/ast"
	"log"
	"reflect"
	"sync/atomic"
)

// 每一个 methodType 实例包含了一个方法的完整信息。包括
// method：方法本身
// ArgType：第一个参数的类型
// ReplyType：第二个参数的类型
// numCalls：后续统计方法调用次数时会用到
type methodType struct {
	method    reflect.Method
	ArgType   reflect.Type
	ReplyType reflect.Type
	numCalls  uint64
}

func (m *methodType) NumCalls() uint64 {
	return atomic.LoadUint64(&m.numCalls)
}

func (m *methodType) newArgV() reflect.Value {
	var argv reflect.Value
	// arg may be a pointer type, or a value type
	if m.ArgType.Kind() == reflect.Ptr {
		argv = reflect.New(m.ArgType.Elem())
	} else {
		argv = reflect.New(m.ArgType).Elem()
	}
	return argv
}

func (m *methodType) newReplyV() reflect.Value {
	// reply must be a pointer type
	replyV := reflect.New(m.ReplyType.Elem())
	switch m.ReplyType.Elem().Kind() {
	case reflect.Map:
		replyV.Elem().Set(reflect.MakeMap(m.ReplyType.Elem()))
	case reflect.Slice:
		replyV.Elem().Set(reflect.MakeSlice(m.ReplyType.Elem(), 0, 0))
	}
	return replyV
}

// service 的定义也是非常简洁的，name 即映射的结构体的名称，
// 比如 T，比如 WaitGroup；typ 是结构体的类型；rcv 即结构体的实例本身，保留 rcv 是因为在调用时需要 rcv 作为第 0 个参数；
// method 是 map 类型，存储映射的结构体的所有符合条件的方法。
type service struct {
	name   string
	typ    reflect.Type
	rcv    reflect.Value
	method map[string]*methodType
}

// 构造函数 newService，入参是任意需要映射为服务的结构体实例。
func newService(rcv interface{}) *service {
	s := new(service)
	s.rcv = reflect.ValueOf(rcv)
	s.name = reflect.Indirect(s.rcv).Type().Name()
	s.typ = reflect.TypeOf(rcv)
	if !ast.IsExported(s.name) {
		log.Fatalf("rpc server: %s is not a valid service name", s.name)
	}
	s.registerMethods()
	return s
}

// registerMethods 过滤出了符合条件的方法：
// 两个导出或内置类型的入参（反射时为 3 个，第 0 个是自身，类似于 python 的 self，java 中的 this）
// 返回值有且只有 1 个，类型为 error
func (s *service) registerMethods() {
	s.method = make(map[string]*methodType)
	for i := 0; i < s.typ.NumMethod(); i++ {
		method := s.typ.Method(i)
		mType := method.Type
		if mType.NumIn() != 3 || mType.NumOut() != 1 {
			continue
		}
		if mType.Out(0) != reflect.TypeOf((*error)(nil)).Elem() {
			continue
		}
		argType, replyType := mType.In(1), mType.In(2)
		if !isExportedOrBuiltinType(argType) || !isExportedOrBuiltinType(replyType) {
			continue
		}
		s.method[method.Name] = &methodType{
			method:    method,
			ArgType:   argType,
			ReplyType: replyType,
		}
		log.Printf("rpc server: register %s.%s\n", s.name, method.Name)
	}
}

// call 方法，即能够通过反射值调用方法。
func (s *service) call(m *methodType, argv, reply reflect.Value) error {
	atomic.AddUint64(&m.numCalls, 1)
	f := m.method.Func
	returnValues := f.Call([]reflect.Value{s.rcv, argv, reply})
	if errInter := returnValues[0].Interface(); errInter != nil {
		return errInter.(error)
	}
	return nil
}

func isExportedOrBuiltinType(t reflect.Type) bool {
	return ast.IsExported(t.Name()) || t.PkgPath() == ""
}
