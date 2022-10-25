package frugal

import (
	"context"

	"github.com/apache/thrift/lib/go/thrift"
)

// WriteString writes string `value` of field name and id `name` and `field` respectively into `p`.
func WriteString(p thrift.TProtocol, value, name string, field int16) error {
	return WriteStringWithContext(context.Background(), p, value, name, field)
}

// WriteStringWithContext is the same as WriteString with a context.Context.
func WriteStringWithContext(ctx context.Context, p thrift.TProtocol, value, name string, field int16) error {
	if err := p.WriteFieldBegin(ctx, name, thrift.STRING, field); err != nil {
		return thrift.PrependError("write field begin error: ", err)
	}
	if err := p.WriteString(ctx, value); err != nil {
		return thrift.PrependError("field write error: ", err)
	}
	if err := p.WriteFieldEnd(ctx); err != nil {
		return thrift.PrependError("write field end error: ", err)
	}
	return nil
}

// WriteBool writes bool `value` of field name and id `name` and `field` respectively into `p`.
func WriteBool(p thrift.TProtocol, value bool, name string, field int16) error {
	return WriteBoolWithContext(context.Background(), p, value, name, field)
}

// WriteBoolWithContext is the same as WriteBool with a context.Context.
func WriteBoolWithContext(ctx context.Context, p thrift.TProtocol, value bool, name string, field int16) error {
	if err := p.WriteFieldBegin(ctx, name, thrift.BOOL, field); err != nil {
		return thrift.PrependError("write field begin error: ", err)
	}
	if err := p.WriteBool(ctx, value); err != nil {
		return thrift.PrependError("field write error: ", err)
	}
	if err := p.WriteFieldEnd(ctx); err != nil {
		return thrift.PrependError("write field end error: ", err)
	}
	return nil
}

// WriteByte writes byte `value` of field name and id `name` and `field` respectively into `p`.
func WriteByte(p thrift.TProtocol, value int8, name string, field int16) error {
	return WriteByteWithContext(context.Background(), p, value, name, field)
}

// WriteByteWithContext is the same as WriteByte with a context.Context.
func WriteByteWithContext(ctx context.Context, p thrift.TProtocol, value int8, name string, field int16) error {
	if err := p.WriteFieldBegin(ctx, name, thrift.BYTE, field); err != nil {
		return thrift.PrependError("write field begin error: ", err)
	}
	if err := p.WriteByte(ctx, value); err != nil {
		return thrift.PrependError("field write error: ", err)
	}
	if err := p.WriteFieldEnd(ctx); err != nil {
		return thrift.PrependError("write field end error: ", err)
	}
	return nil
}

// WriteDouble writes float64 `value` of field name and id `name` and `field` respectively into `p`.
func WriteDouble(p thrift.TProtocol, value float64, name string, field int16) error {
	return WriteDoubleWithContext(context.Background(), p, value, name, field)
}

// WriteDoubleWithContext is the same as WriteDouble with a context.Context.
func WriteDoubleWithContext(ctx context.Context, p thrift.TProtocol, value float64, name string, field int16) error {
	if err := p.WriteFieldBegin(ctx, name, thrift.DOUBLE, field); err != nil {
		return thrift.PrependError("write field begin error: ", err)
	}
	if err := p.WriteDouble(ctx, value); err != nil {
		return thrift.PrependError("field write error: ", err)
	}
	if err := p.WriteFieldEnd(ctx); err != nil {
		return thrift.PrependError("write field end error: ", err)
	}
	return nil
}

// WriteI16 writes int16 `value` of field name and id `name` and `field` respectively into `p`.
func WriteI16(p thrift.TProtocol, value int16, name string, field int16) error {
	return WriteI16WithContext(context.Background(), p, value, name, field)
}

// WriteI16WithContext is the same as WriteI16 with a context.Context.
func WriteI16WithContext(ctx context.Context, p thrift.TProtocol, value int16, name string, field int16) error {
	if err := p.WriteFieldBegin(ctx, name, thrift.I16, field); err != nil {
		return thrift.PrependError("write field begin error: ", err)
	}
	if err := p.WriteI16(ctx, value); err != nil {
		return thrift.PrependError("field write error: ", err)
	}
	if err := p.WriteFieldEnd(ctx); err != nil {
		return thrift.PrependError("write field end error: ", err)
	}
	return nil
}

// WriteI32 writes int32 `value` of field name and id `name` and `field` respectively into `p`.
func WriteI32(p thrift.TProtocol, value int32, name string, field int16) error {
	return WriteI32WithContext(context.Background(), p, value, name, field)
}

// WriteI32WithContext is the same as WriteI32 with a context.Context.
func WriteI32WithContext(ctx context.Context, p thrift.TProtocol, value int32, name string, field int16) error {
	if err := p.WriteFieldBegin(ctx, name, thrift.I32, field); err != nil {
		return thrift.PrependError("write field begin error: ", err)
	}
	if err := p.WriteI32(ctx, value); err != nil {
		return thrift.PrependError("field write error: ", err)
	}
	if err := p.WriteFieldEnd(ctx); err != nil {
		return thrift.PrependError("write field end error: ", err)
	}
	return nil
}

// WriteI64 writes int64 `value` of field name and id `name` and `field` respectively into `p`.
func WriteI64(p thrift.TProtocol, value int64, name string, field int16) error {
	return WriteI64WithContext(context.Background(), p, value, name, field)
}

// WriteI64WithContext is the same as WriteI64 with a context.Context.
func WriteI64WithContext(ctx context.Context, p thrift.TProtocol, value int64, name string, field int16) error {
	if err := p.WriteFieldBegin(ctx, name, thrift.I64, field); err != nil {
		return thrift.PrependError("write field begin error: ", err)
	}
	if err := p.WriteI64(ctx, value); err != nil {
		return thrift.PrependError("field write error: ", err)
	}
	if err := p.WriteFieldEnd(ctx); err != nil {
		return thrift.PrependError("write field end error: ", err)
	}
	return nil
}

// WriteBinary writes []byte `value` of field name and id `name` and `field` respectively into `p`.
func WriteBinary(p thrift.TProtocol, value []byte, name string, field int16) error {
	return WriteBinaryWithContext(context.Background(), p, value, name, field)
}

// WriteBinaryWithContext is the same as WriteBinary with a context.Context.
func WriteBinaryWithContext(ctx context.Context, p thrift.TProtocol, value []byte, name string, field int16) error {
	if err := p.WriteFieldBegin(ctx, name, thrift.STRING, field); err != nil {
		return thrift.PrependError("write field begin error: ", err)
	}
	if err := p.WriteBinary(ctx, value); err != nil {
		return thrift.PrependError("field write error: ", err)
	}
	if err := p.WriteFieldEnd(ctx); err != nil {
		return thrift.PrependError("write field end error: ", err)
	}
	return nil
}

// WriteStruct writes thrift.Struct of filed and id `name` and `field` respectively into `p`.
func WriteStruct(p thrift.TProtocol, value thrift.TStruct, name string, field int16) error {
	return WriteStructWithContext(context.Background(), p, value, name, field)
}

// WriteStructWithContext is the same as WriteStruct with a context.Context.
func WriteStructWithContext(ctx context.Context, p thrift.TProtocol, value thrift.TStruct, name string, field int16) error {
	if err := p.WriteFieldBegin(ctx, name, thrift.STRUCT, field); err != nil {
		return thrift.PrependError("write field begin error: ", err)
	}
	if err := value.Write(ctx, p); err != nil {
		return thrift.PrependError("field write error: ", err)
	}
	if err := p.WriteFieldEnd(ctx); err != nil {
		return thrift.PrependError("write field end error: ", err)
	}
	return nil
}
