## Eunomia - Lightweight persistent file based queue

- Eunomia is a simple file based queue, that allows you to keep local tasks and continue them
even if your process crashes, allowing for safer workloads.


## Usage

- The usage is pretty straight forward you can start by doing 
```
go get https://github.com/chermehdi/eunomia
```
in your project root directory

```go
queue := eunomia.NewQueue("queue-name")
queue.Add(serializableStruct)
queue.Add(serializableStruct)

queueLength := queue.Size() // 2
element := queue.Peek()
queueLength := queue.Size() // 2

element = queue.Poll()
queueLength := queue.Size() // 1
```
