package eunomia

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
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

func BenchmarkFileQueue_Peek_Complex(b *testing.B) {
	queue := QueueSetup(ElementCount, func(i int) interface{} {
		return ComplexStructure{
			FirstName: "John",
			LastName:  fmt.Sprintf("Doe %v", i),
			Address: &Address{
				StreetName: "Backer street",
				PostalCode: "AB 123",
				City:       "London",
			},
			Phone: &PhoneNumber{
				Home: "+44 7911 123456",
				Work: "44 7911 887676",
			},
			Age: i + 10,
		}
	}, &ComplexSerializer{})
	defer queue.Delete()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		optimisationPreventer, _ = queue.Peek()
	}
}

func BenchmarkFileQueue_Poll_Complex(b *testing.B) {
	queue := QueueSetup(b.N, func(i int) interface{} {
		return ComplexStructure{
			FirstName: "John",
			LastName:  fmt.Sprintf("Doe %v", i),
			Address: &Address{
				StreetName: "Backer street",
				PostalCode: "AB 123",
				City:       "London",
			},
			Phone: &PhoneNumber{
				Home: "+44 7911 123456",
				Work: "44 7911 887676",
			},
			Age: i + 10,
		}
	}, &ComplexSerializer{})
	defer queue.Delete()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		optimisationPreventer, _ = queue.Poll()
	}
}
func BenchmarkFileQueue_Push_Complex(b *testing.B) {
	queue, _ := NewFileQueue("bench-queue", &ComplexSerializer{})
	defer queue.Delete()

	b.ResetTimer()
	complex := ComplexStructure{
		FirstName: "John",
		LastName:  "doe",
		Address: &Address{
			StreetName: "Backer street",
			PostalCode: "AB 123",
			City:       "London",
		},
		Phone: &PhoneNumber{
			Home: "+44 7911 123456",
			Work: "44 7911 887676",
		},
		Age: 26,
	}
	for i := 0; i < b.N; i++ {
		if err := queue.Push(complex); err != nil {
			panic(err)
		}
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
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	if err := encoder.Encode(i); err != nil {
		panic(err)
	}
	file, err := os.OpenFile("test-file", os.O_CREATE|os.O_RDWR, 0777)
	if err != nil {
		panic(file)
	}
	defer os.Remove("test-file")
	_, err = WriteString(file, 0, buffer.String())
	if err != nil {
		panic(err)
	}
	res, err := ioutil.ReadAll(file)
	if err != nil {
		panic(err)
	}
	return res
}

func (c ComplexSerializer) Read(reader io.Reader) interface{} {
	buffer, err := ioutil.ReadAll(reader)
	if err != nil {
		panic(err)
	}
	if err = ioutil.WriteFile("test-file", buffer, 0777); err != nil {
		panic(err)
	}
	defer os.Remove("test-file")
	file, err := os.Open("test-file")
	if err != nil {
		panic(err)
	}
	value, err := ReadString(file, 0)
	if err != nil {
		panic(err)
	}
	decoder := json.NewDecoder(strings.NewReader(value))
	result := ComplexStructure{}
	if err := decoder.Decode(&result); err != nil {
		panic(err)
	}
	return result
}
