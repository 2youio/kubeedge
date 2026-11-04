/*
Copyright 2026 The KubeEdge Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package handlerfactory

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"sync"

	gogoproto "github.com/gogo/protobuf/proto"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

// descriptorMessage is implemented by the generated protobuf code of every built-in type.
type descriptorMessage interface {
	Descriptor() ([]byte, []int)
}

// protoMessages indexes the message descriptors of the built-in types by full
// name, such as ".k8s.io.api.core.v1.Node". Files are loaded on first use.
type protoMessages struct {
	mu       sync.Mutex
	files    map[string]bool
	messages map[string]*descriptorpb.DescriptorProto
}

var builtInMessages = &protoMessages{
	files:    map[string]bool{},
	messages: map[string]*descriptorpb.DescriptorProto{},
}

// messageOf returns the descriptor of obj's protobuf message.
func (p *protoMessages) messageOf(obj interface{}) (*descriptorpb.DescriptorProto, error) {
	d, ok := obj.(descriptorMessage)
	if !ok {
		return nil, fmt.Errorf("%T has no protobuf descriptor", obj)
	}
	gz, path := d.Descriptor()
	fd, err := parseFileDescriptor(gz)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.load(fd); err != nil {
		return nil, err
	}
	name := "." + fd.GetPackage()
	messages := fd.GetMessageType()
	for _, i := range path {
		if i < 0 || i >= len(messages) {
			return nil, fmt.Errorf("bad descriptor path %v for %T", path, obj)
		}
		name += "." + messages[i].GetName()
		messages = messages[i].GetNestedType()
	}
	return p.messages[name], nil
}

func (p *protoMessages) lookup(name string) (*descriptorpb.DescriptorProto, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	m, ok := p.messages[name]
	if !ok {
		return nil, fmt.Errorf("no protobuf descriptor for message %s", name)
	}
	return m, nil
}

// load indexes fd and, through gogo's file registry, every registered file it
// imports. The caller holds p.mu.
func (p *protoMessages) load(fd *descriptorpb.FileDescriptorProto) error {
	if p.files[fd.GetName()] {
		return nil
	}
	p.files[fd.GetName()] = true
	p.index("."+fd.GetPackage(), fd.GetMessageType())
	for _, dep := range fd.GetDependency() {
		if p.files[dep] {
			continue
		}
		gz := gogoproto.FileDescriptor(dep)
		if gz == nil {
			// Such as gogo.proto, which holds only options. A message type that
			// is really missing fails in lookup.
			continue
		}
		depFd, err := parseFileDescriptor(gz)
		if err != nil {
			return err
		}
		if err := p.load(depFd); err != nil {
			return err
		}
	}
	return nil
}

func (p *protoMessages) index(prefix string, messages []*descriptorpb.DescriptorProto) {
	for _, m := range messages {
		name := prefix + "." + m.GetName()
		p.messages[name] = m
		p.index(name, m.GetNestedType())
	}
}

func parseFileDescriptor(gz []byte) (*descriptorpb.FileDescriptorProto, error) {
	r, err := gzip.NewReader(bytes.NewReader(gz))
	if err != nil {
		return nil, fmt.Errorf("failed to read a protobuf file descriptor: %w", err)
	}
	b, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("failed to read a protobuf file descriptor: %w", err)
	}
	fd := &descriptorpb.FileDescriptorProto{}
	if err := proto.Unmarshal(b, fd); err != nil {
		return nil, fmt.Errorf("failed to parse a protobuf file descriptor: %w", err)
	}
	return fd, nil
}

// droppedFields returns the paths of the fields in b, an encoded message of type
// m, that m lacks and that hold a value. Decoding into m would lose them. A
// lacking field with a zero value is skipped: newer clients always send some.
func (p *protoMessages) droppedFields(b []byte, m *descriptorpb.DescriptorProto, path string, dropped []string) ([]string, error) {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		b = b[n:]
		n = protowire.ConsumeFieldValue(num, typ, b)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		value := b[:n]
		b = b[n:]

		field := fieldByNumber(m, num)
		switch {
		case field == nil:
			if !isZeroValue(typ, value) {
				dropped = append(dropped, fmt.Sprintf("%s(field %d)", path, num))
			}
		case field.GetType() == descriptorpb.FieldDescriptorProto_TYPE_MESSAGE && typ == protowire.BytesType:
			fieldMessage, err := p.lookup(field.GetTypeName())
			if err != nil {
				return nil, err
			}
			inner, _ := protowire.ConsumeBytes(value)
			if dropped, err = p.droppedFields(inner, fieldMessage, path+field.GetName()+".", dropped); err != nil {
				return nil, err
			}
		}
	}
	return dropped, nil
}

func fieldByNumber(m *descriptorpb.DescriptorProto, num protowire.Number) *descriptorpb.FieldDescriptorProto {
	for _, f := range m.GetField() {
		if protowire.Number(f.GetNumber()) == num {
			return f
		}
	}
	return nil
}

// isZeroValue reports whether a field value is 0, empty, or a message whose
// fields are all zero. Without the schema an empty string and an empty message look alike.
func isZeroValue(typ protowire.Type, value []byte) bool {
	switch typ {
	case protowire.VarintType:
		v, _ := protowire.ConsumeVarint(value)
		return v == 0
	case protowire.Fixed32Type:
		v, _ := protowire.ConsumeFixed32(value)
		return v == 0
	case protowire.Fixed64Type:
		v, _ := protowire.ConsumeFixed64(value)
		return v == 0
	case protowire.BytesType:
		inner, _ := protowire.ConsumeBytes(value)
		for len(inner) > 0 {
			num, innerTyp, n := protowire.ConsumeTag(inner)
			if n < 0 {
				return false
			}
			inner = inner[n:]
			n = protowire.ConsumeFieldValue(num, innerTyp, inner)
			if n < 0 || !isZeroValue(innerTyp, inner[:n]) {
				return false
			}
			inner = inner[n:]
		}
		return true
	}
	return false
}
