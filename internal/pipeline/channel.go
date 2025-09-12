package pipeline

type ChannelPipe struct {
	in  chan Msg
	out chan Msg

	done chan struct{}
}

func NewChanPipe() *ChannelPipe {
	return &ChannelPipe{
		in:   make(chan Msg, 1),
		out:  make(chan Msg, 1),
		done: make(chan struct{}),
	}
}

func (c *ChannelPipe) Done() <-chan struct{} {
	return c.done
}

func (c *ChannelPipe) In() chan Msg {
	return c.in
}

func (c *ChannelPipe) SetInChan(cin chan Msg) {
	c.in = cin
}

func (c *ChannelPipe) Out() chan Msg {
	return c.out
}

func (c *ChannelPipe) SetOutChan(cout chan Msg) {
	c.out = cout
}

func (c *ChannelPipe) Chain(p Pipe) {
	c.out = p.In()
}

func (c *ChannelPipe) Close() error {
	SafeClose(c.done)
	SafeClose(c.out)

	return nil
}

func SafeClose[T any](ch chan T) (justClosed bool) {
	defer func() {
		if recover() != nil {
			justClosed = false
		}
	}()
	close(ch)
	return true
}
