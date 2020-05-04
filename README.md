## Eunomia - Lightweight persistent file based queue

- Eunomia is a simple file based queue, that allows you to keep local tasks and continue them
even if your process crashes, allowing for safer workloads.

## Usage

- The usage is pretty straight forward you can start by doing 
```
go get https://github.com/chermehdi/eunomia
```
- This is the simplest possible example:

```go
queue := eunomia.NewQueue("queue-name")
queue.Add(serializableStruct)
queue.Add(serializableStruct)

queueLength := queue.Size() // 2
element := queue.Peek()
queueLength := queue.Size() // 2

element = queue.Poll()
queueLength := queue.Size() // 1

queue.Delete() // dangerous, will delete the file
```

- The only constraint on your data is that it should adhere to the `QueueElement` interface, which simply tells
the queue implementation how to serialize your data from and to a byte stream.

## Contribution
- You can contribute with creating issues, or solving them by submitting PR's or just by adding feature requests.
