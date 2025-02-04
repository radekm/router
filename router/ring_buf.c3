// Ring buffer from `std::collections::ringbuffer` seemed strange, not like a queue but like a stack.
<*
 @require values::@is_int(SIZE) &&& SIZE > 0 "The size must be positive integer"
*>
module router::ringbuf(<Type, SIZE>);

struct RingBuf
{
    int head;  // Index of the item to dequeue.
    int written;  // Number of currently enqueued items.
    Type[SIZE] buf;
}

fn bool RingBuf.can_enqueue(&self)
{
    return self.written < SIZE;
}

fn void RingBuf.enqueue(&self, Type x)
{
    if (self.written == SIZE)
    {
        abort("Ring buffer is full");
    }
    self.buf[(self.head + self.written) % SIZE] = x;
    self.written++;
}

fn Type RingBuf.dequeue(&self)
{
    if (self.written == 0)
    {
        abort("Ring buffer is empty");
    }
    Type x = self.buf[self.head];
    self.head = (self.head + 1) % SIZE;
    self.written--;
    return x;
}

fn Type RingBuf.peek(&self)
{
    if (self.written == 0)
    {
        abort("Ring buffer is empty");
    }
    Type x = self.buf[self.head];
    return x;
}
