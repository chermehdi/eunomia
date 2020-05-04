# Eunomia - Lightweight persistent file based queue

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

## How Eunomia stores data?

### Serialisation format

- Eunomia writes data to disk in the following format.
- Every row is `8 bytes`

```
0       1       2       3       4       5       6       7 
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
      Version                   |           Flags
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
                           Element count
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
                           Head offset
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
                           Tail offset
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
                          Queue elements ... 
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

- `version`: A number for backward compatibilty guarentees, changing the number of the version can mean breaking changes 
in the encoding format if we in future version change the encoding format by updating some offset, users can only update
existing queue files if the version written to the file matches the one in the queue library.
- `flags`: Gives (potential) additional information on how the format of the queue (bounded, compressed ...)
- `Element count`: The number of elements currently in the queue.
- `Head offset`: The index of the head element, it points to the element that will be seen after a call to `Peek`
- `Tail offset`: The index of the tail element, it points to the last element in the queue, calling `Add` will update the
tail to point to the next location.
- Each `QueueElement` is encoded as:

```
0       1       2       3       4       5       6       7 
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
                           Element length 
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
                           Element data ...


+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```
## Contribution
- You can contribute with creating issues, or solving them by submitting PR's or just by adding feature requests.
