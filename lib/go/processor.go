/*
 * Copyright 2017 Workiva
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package frugal

import (
	"context"
	"sync"

	"github.com/apache/thrift/lib/go/thrift"
)

// FProcessor is Frugal's equivalent of Thrift's TProcessor. It's a generic
// object which operates upon an input stream and writes to an output stream.
// Specifically, an FProcessor is provided to an FServer in order to wire up a
// service handler to process requests.
type FProcessor interface {
	// Process the request from the input protocol and write the response to
	// the output protocol.
	Process(in, out *FProtocol) error

	// AddMiddleware adds the given ServiceMiddleware to the FProcessor. This
	// should only be called before the server is started.
	AddMiddleware(ServiceMiddleware)

	// Annotations returns a map of method name to annotations as defined in
	// the service IDL that is serviced by this processor.
	Annotations() map[string]map[string]string
}

// FBaseProcessor is a base implementation of FProcessor. FProcessors should
// embed this and register FProcessorFunctions. This should only be used by
// generated code.
type FBaseProcessor struct {
	writeMu        sync.Mutex
	processMap     map[string]FProcessorFunction
	annotationsMap map[string]map[string]string
}

// NewFBaseProcessor returns a new FBaseProcessor which FProcessors can extend.
func NewFBaseProcessor() *FBaseProcessor {
	return &FBaseProcessor{
		processMap:     make(map[string]FProcessorFunction),
		annotationsMap: make(map[string]map[string]string),
	}
}

// Process the request from the input protocol and write the response to the
// output protocol.
func (f *FBaseProcessor) Process(iprot, oprot *FProtocol) error {
	fctx, err := iprot.ReadRequestHeader()
	if err != nil {
		return err
	}
	ctx, cancelFn := ToContext(fctx)
	defer cancelFn()
	name, _, _, err := iprot.ReadMessageBegin(ctx)
	if err != nil {
		return err
	}
	if processor, ok := f.processMap[name]; ok {
		if err := processor.Process(fctx, iprot, oprot); err != nil {
			if _, ok := err.(thrift.TException); ok {
				logger().Errorf(
					"frugal: error occurred while processing request with correlation id %s: %s",
					fctx.CorrelationID(), err.Error())
			} else {
				logger().Errorf(
					"frugal: user handler code returned unhandled error on request with correlation id %s: %s",
					fctx.CorrelationID(), err.Error())
			}
		}
		// Return nil because the server should still send a response to the client.
		return nil
	}

	logger().Warnf("frugal: client invoked unknown function %s on request with correlation id %s",
		name, fctx.CorrelationID())
	if err := iprot.Skip(ctx, thrift.STRUCT); err != nil {
		return err
	}
	if err := iprot.ReadMessageEnd(ctx); err != nil {
		return err
	}
	ex := thrift.NewTApplicationException(APPLICATION_EXCEPTION_UNKNOWN_METHOD, "Unknown function "+name)
	f.writeMu.Lock()
	defer f.writeMu.Unlock()
	if err := oprot.WriteResponseHeader(fctx); err != nil {
		return err
	}
	if err := oprot.WriteMessageBegin(ctx, name, thrift.EXCEPTION, 0); err != nil {
		return err
	}
	if err := ex.Write(ctx, oprot); err != nil {
		return err
	}
	if err := oprot.WriteMessageEnd(ctx); err != nil {
		return err
	}
	if err := oprot.Flush(ctx); err != nil {
		return err
	}
	return nil
}

// AddMiddleware adds the given ServiceMiddleware to the FProcessor. This
// should only be called before the server is started.
func (f *FBaseProcessor) AddMiddleware(middleware ServiceMiddleware) {
	for _, p := range f.processMap {
		p.AddMiddleware(middleware)
	}
}

// AddToProcessorMap registers the given FProcessorFunction.
func (f *FBaseProcessor) AddToProcessorMap(key string, proc FProcessorFunction) {
	f.processMap[key] = proc
}

// AddToAnnotationsMap registers the given annotations to the given method.
func (f *FBaseProcessor) AddToAnnotationsMap(method string, annotations map[string]string) {
	f.annotationsMap[method] = annotations
}

// Annotations returns a map of method name to annotations as defined in
// the service IDL that is serviced by this processor.
func (f *FBaseProcessor) Annotations() map[string]map[string]string {
	annoCopy := make(map[string]map[string]string)
	for k, v := range f.annotationsMap {
		methodCopy := make(map[string]string)
		for mk, mv := range v {
			methodCopy[mk] = mv
		}
		annoCopy[k] = methodCopy
	}
	return annoCopy
}

// GetWriteMutex returns the Mutex which FProcessorFunctions should use to
// synchronize access to the output FProtocol.
func (f *FBaseProcessor) GetWriteMutex() *sync.Mutex {
	return &f.writeMu
}

// FProcessorFunction is used internally by generated code. An FProcessor
// registers an FProcessorFunction for each service method. Like FProcessor, an
// FProcessorFunction exposes a single process call, which is used to handle a
// method invocation.
type FProcessorFunction interface {
	// Process the request from the input protocol and write the response to
	// the output protocol.
	Process(ctx FContext, in, out *FProtocol) error

	// AddMiddleware adds the given ServiceMiddleware to the
	// FProcessorFunction. This should only be called before the server is
	// started.
	AddMiddleware(middleware ServiceMiddleware)
}

// FBaseProcessorFunction is a base implementation of FProcessorFunction.
// FProcessorFunctions should embed this. This should only be used by generated
// code.
type FBaseProcessorFunction struct {
	handler *Method
	writeMu *sync.Mutex
}

// NewFBaseProcessorFunction returns a new FBaseProcessorFunction which
// FProcessorFunctions can extend.
func NewFBaseProcessorFunction(writeMu *sync.Mutex, handler *Method) *FBaseProcessorFunction {
	return &FBaseProcessorFunction{handler, writeMu}
}

// GetWriteMutex returns the Mutex which should be used to synchronize access
// to the output FProtocol.
//
// Deprecated: use SendError or SendReply instead!
func (f *FBaseProcessorFunction) GetWriteMutex() *sync.Mutex {
	return f.writeMu
}

// AddMiddleware adds the given ServiceMiddleware to the FProcessorFunction.
// This should only be called before the server is started.
func (f *FBaseProcessorFunction) AddMiddleware(middleware ServiceMiddleware) {
	f.handler.AddMiddleware(middleware)
}

// InvokeMethod invokes the handler method.
func (f *FBaseProcessorFunction) InvokeMethod(args []interface{}) Results {
	return f.handler.Invoke(args)
}

// SendError writes the error to the desired transport
func (f *FBaseProcessorFunction) SendError(fctx FContext, oprot *FProtocol, kind int32, method, message string) error {
	ctx, cancelFn := ToContext(fctx)
	defer cancelFn()
	f.writeMu.Lock()
	err := f.sendError(ctx, fctx, oprot, kind, method, message)
	f.writeMu.Unlock()
	return err
}

func (f *FBaseProcessorFunction) sendError(ctx context.Context, fctx FContext, oprot *FProtocol, kind int32, method, message string) error {
	err := thrift.NewTApplicationException(kind, message)
	oprot.WriteResponseHeader(fctx)
	oprot.WriteMessageBegin(ctx, method, thrift.EXCEPTION, 0)
	err.Write(ctx, oprot)
	oprot.WriteMessageEnd(ctx)
	oprot.Flush(ctx)
	return err
}

// SendReply ...
func (f *FBaseProcessorFunction) SendReply(fctx FContext, oprot *FProtocol, method string, result thrift.TStruct) error {
	f.writeMu.Lock()
	defer f.writeMu.Unlock()
	ctx, cancelFn := ToContext(fctx)
	defer cancelFn()
	if err := oprot.WriteResponseHeader(fctx); err != nil {
		return f.trapError(ctx, fctx, oprot, method, err)
	}
	if err := oprot.WriteMessageBegin(ctx, method, thrift.REPLY, 0); err != nil {
		return f.trapError(ctx, fctx, oprot, method, err)
	}
	if err := result.Write(ctx, oprot); err != nil {
		return f.trapError(ctx, fctx, oprot, method, err)
	}
	if err := oprot.WriteMessageEnd(ctx); err != nil {
		return f.trapError(ctx, fctx, oprot, method, err)
	}
	if err := oprot.Flush(ctx); err != nil {
		return f.trapError(ctx, fctx, oprot, method, err)
	}
	return nil
}

func (f *FBaseProcessorFunction) trapError(ctx context.Context, fctx FContext, oprot *FProtocol, method string, err error) error {
	if IsErrTooLarge(err) {
		f.sendError(ctx, fctx, oprot, APPLICATION_EXCEPTION_RESPONSE_TOO_LARGE, method, err.Error())
		return nil
	}
	return err
}
