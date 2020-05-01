package eunomia

import "io"

type Queue interface {
  Push(element QueueElement)

  Poll() (QueueElement, error)

  Size() int64
}

type Serializable interface {

  Write(writer io.Writer)

  Read(reader io.Reader)
}

type QueueElement interface {
  Serializable
}
