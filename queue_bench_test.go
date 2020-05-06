package eunomia

import (
	"io"
	"io/ioutil"
	"os"
	"testing"
)

const ElementCount = 1

type ElementFactory func(int) interface{}

// Store local values so that the compiler does not eliminate function calls.
var optimisationPreventer interface{}

func QueueSetup(n int, factory ElementFactory, serializer Serializer) Queue {
	queue, _ := NewFileQueue("bench-queue", serializer)
	for i := 0; i < n; i++ {
		if err := queue.Push(factory(i)); err != nil {
			panic(err)
		}
	}
	return queue
}

func BenchmarkFileQueue_Push_Simple(b *testing.B) {
	queue, _ := NewFileQueue("bench-queue", &MockDataSerializer{})
	defer queue.Delete()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := queue.Push(MockData{int32(i)}); err != nil {
			panic(err)
		}
	}
}

func BenchmarkFileQueue_Peek_Simple(b *testing.B) {
	queue := QueueSetup(ElementCount, func(i int) interface{} {
		return MockData{int32(i)}
	}, &MockDataSerializer{})
	defer queue.Delete()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		optimisationPreventer, _ = queue.Peek()
	}
}

func BenchmarkFileQueue_Poll_Simple(b *testing.B) {
	queue := QueueSetup(b.N, func(i int) interface{} {
		return MockData{int32(i)}
	}, &MockDataSerializer{})
	defer queue.Delete()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		optimisationPreventer, _ = queue.Poll()
	}
}

type Address struct {
	StreetName string
	PostalCode string
	City       string
}

type PhoneNumber struct {
	Home string
	Work string
}

type ComplexStructure struct {
	FirstName string
	LastName  string
	Address   *Address
	Phone     *PhoneNumber
	Age       int
}

type ComplexSerializer struct {
}

func (c *ComplexSerializer) Write(i interface{}) []byte {
	tempFile, err := os.Open("file")
	defer os.Remove("file")

	if err != nil {
		panic(err.Error())
	}

	data, ok := i.(ComplexStructure)
	if !ok {
		panic("Unexpected type")
	}
	currOffset := int64(0)
	currOffset, _ = WriteString(tempFile, currOffset, data.FirstName)
	currOffset, _ = WriteString(tempFile, currOffset, data.LastName)
	currOffset, _ = WriteString(tempFile, currOffset, data.Address.City)
	currOffset, _ = WriteString(tempFile, currOffset, data.Address.PostalCode)
	currOffset, _ = WriteString(tempFile, currOffset, data.Address.StreetName)
	currOffset, _ = WriteString(tempFile, currOffset, data.Phone.Home)
	currOffset, _ = WriteString(tempFile, currOffset, data.Phone.Work)
	currOffset, _ = WriteInt(tempFile, currOffset, int32(data.Age))
	result, _ := ioutil.ReadAll(tempFile)
	return result
}

func (c ComplexSerializer) Read(reader io.Reader) interface{} {
	result := ComplexStructure{}
	buffer, err := ioutil.ReadAll(reader)
	if err != nil {
		panic(err)
	}
}
