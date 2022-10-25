package frugal

import (
	"context"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

var (
	_ FPublisherTransport = (*mockFPublisherTransport)(nil)
	_ thrift.TStruct      = (*mockTStruct)(nil)
)

type mockFPublisherTransport struct {
	mock.Mock
}

func (m *mockFPublisherTransport) Open() error               { return m.Called().Error(0) }
func (m *mockFPublisherTransport) Close() error              { return m.Called().Error(0) }
func (m *mockFPublisherTransport) IsOpen() bool              { return m.Called().Bool(0) }
func (m *mockFPublisherTransport) GetPublishSizeLimit() uint { return m.Called().Get(0).(uint) }
func (m *mockFPublisherTransport) Publish(topic string, body []byte) error {
	return m.Called(topic, body).Error(0)
}

type mockTStruct struct {
	mock.Mock
}

func (m *mockTStruct) Read(ctx context.Context, iprot thrift.TProtocol) error {
	return m.Called(iprot).Error(0)
}
func (m *mockTStruct) Write(ctx context.Context, oprot thrift.TProtocol) error {
	return m.Called(oprot).Error(0)
}

func TestFStandardClient(t *testing.T) {
	transport := new(mockFTransport)
	mockTProtocolFactory := new(mockTProtocolFactory)
	protoFactory := NewFProtocolFactory(mockTProtocolFactory)
	provider := NewFServiceProvider(transport, protoFactory)
	transport.On("GetRequestSizeLimit").Return(uint(100))

	out := NewFStandardClient(provider)
	assert.Equal(t, out.transport, transport)
	assert.Equal(t, out.protocolFactory, protoFactory)
	assert.Equal(t, out.limit, uint(100))
}

func TestFScopeClient(t *testing.T) {
	mockPublisherTransportFactory := new(mockFPublisherTransportFactory)
	mockTProtocolFactory := new(mockTProtocolFactory)
	protoFactory := NewFProtocolFactory(mockTProtocolFactory)
	provider := NewFScopeProvider(mockPublisherTransportFactory, nil, protoFactory)
	publisherTransport := new(fNatsPublisherTransport)
	mockPublisherTransportFactory.On("GetTransport").Return(publisherTransport)

	out := NewFScopeClient(provider)
	assert.Equal(t, out.publisher, publisherTransport)
	assert.Equal(t, out.protocolFactory, protoFactory)
	assert.Equal(t, out.limit, uint(natsMaxMessageSize))
}

func TestFClientOpen(t *testing.T) {
	publisher := new(mockFPublisherTransport)
	client := &FStandardClient{publisher: publisher}
	publisher.On("Open").Return(nil)
	assert.NoError(t, client.Open())
	publisher.AssertExpectations(t)
}

func TestFClientClose(t *testing.T) {
	publisher := new(mockFPublisherTransport)
	client := &FStandardClient{publisher: publisher}
	publisher.On("Close").Return(nil)
	assert.NoError(t, client.Close())
	publisher.AssertExpectations(t)
}

func TestFClientPublish(t *testing.T) {
	obj := new(mockTStruct)
	publisher := new(mockFPublisherTransport)
	mockTProtocolFactory := new(mockTProtocolFactory)
	protoFactory := NewFProtocolFactory(mockTProtocolFactory)
	client := &FStandardClient{
		publisher:       publisher,
		protocolFactory: protoFactory,
	}
	mockTransport := new(mockFTransport)
	proto := thrift.NewTJSONProtocol(mockTransport)
	ctx := NewFContext("uuid")

	mockTProtocolFactory.On("GetProtocol", mock.Anything).Return(proto)
	mockTransport.On("Write", mock.Anything).Return(55, nil)
	obj.On("Write", mock.Anything).Return(nil)
	mockTransport.On("Flush", mock.Anything).Return(nil)
	publisher.On("Publish", "topic", []byte{0, 0, 0, 0}).Return(nil)

	assert.NoError(t, client.Publish(ctx, "op", "topic", obj))

	publisher.AssertExpectations(t)
	mockTProtocolFactory.AssertExpectations(t)
	mockTransport.AssertExpectations(t)
	obj.AssertExpectations(t)
}

func TestFClientOneway(t *testing.T) {
	ctx := NewFContext("uuid")
	obj := new(mockTStruct)
	mockTProtocolFactory := new(mockTProtocolFactory)
	protoFactory := NewFProtocolFactory(mockTProtocolFactory)
	transport := new(mockFTransport)
	client := &FStandardClient{
		transport:       transport,
		protocolFactory: protoFactory,
	}
	mockTransport := new(mockFTransport)
	proto := thrift.NewTJSONProtocol(mockTransport)

	mockTProtocolFactory.On("GetProtocol", mock.Anything).Return(proto)
	mockTransport.On("Write", mock.Anything).Return(55, nil)
	obj.On("Write", mock.Anything).Return(nil)
	mockTransport.On("Flush", mock.Anything).Return(nil)
	transport.On("Oneway").Return(nil)

	assert.NoError(t, client.Oneway(ctx, "method", obj))

	obj.AssertExpectations(t)
	mockTProtocolFactory.AssertExpectations(t)
	mockTransport.AssertExpectations(t)
	transport.AssertExpectations(t)
}

func TestFClientCall(t *testing.T) {
	ctx := NewFContext("uuid")
	args := new(mockTStruct)
	result := new(mockTStruct)
	mockTProtocolFactory := new(mockTProtocolFactory)
	protoFactory := NewFProtocolFactory(mockTProtocolFactory)
	transport := new(mockFTransport)
	client := &FStandardClient{
		transport:       transport,
		protocolFactory: protoFactory,
	}
	mockTransport := new(mockFTransport)
	proto := thrift.NewTJSONProtocol(mockTransport)
	resultTransport := new(mockFTransport)

	mockTProtocolFactory.On("GetProtocol", mock.Anything).Return(proto)
	mockTransport.On("Write", mock.Anything).Return(55, nil)
	args.On("Write", mock.Anything).Return(nil)
	mockTransport.On("Flush", mock.Anything).Return(nil)
	transport.On("Request").Return(resultTransport, nil)
	calls := 0
	output := append([]byte{0, 0, 0, 0, 0}, []byte(`[1, "method", 2, 0]`)...)
	mockTransport.On("Read", mock.Anything).Run(func(args mock.Arguments) {
		args.Get(0).([]byte)[0] = output[calls]
		calls++
	}).Return(1, nil)
	mockTransport.On("Read", mock.Anything).Run(func(args mock.Arguments) { copy(args.Get(0).([]byte), []byte("[]")) }).Return(2, nil).Once()
	result.On("Read", mock.Anything).Return(nil)

	assert.NoError(t, client.Call(ctx, "method", args, result))

	args.AssertExpectations(t)
	result.AssertExpectations(t)
	mockTProtocolFactory.AssertExpectations(t)
	transport.AssertExpectations(t)
	resultTransport.AssertExpectations(t)
}
