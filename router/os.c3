module router;

import std::net::os;
import libc;

const AISockType SOCK_NONBLOCK = 0o4000;  // Since kernel 2.6.27.

const SignalFunction SIG_IGN = (SignalFunction)1;
const SignalFunction SIG_ERR = (SignalFunction)(uptr)(iptr)-1;

const CInt EAI_SYSTEM = -11;

extern fn ZString gai_strerror(CInt ecode);

extern fn NativeSocket accept4(NativeSocket socket, SockAddrPtr addr, Socklen_t* addr_len, AISockType flags);

extern fn CInt getnameinfo(
    SockAddrPtr addr, Socklen_t addr_len,
    char* host, Socklen_t host_len,
    char* serv, Socklen_t serv_len,
    CInt flags);

distinct EPollFd = inline Fd;

fn void EPollFd.close(self)
{
    libc::close(self);
}

distinct EPollCtl = CInt;
const EPollCtl EPOLL_CTL_ADD = 1;
const EPollCtl EPOLL_CTL_DEL = 2;
const EPollCtl EPOLL_CTL_MOD = 3;

distinct EPoll = uint;
const EPoll EPOLLIN = 0x001;
const EPoll EPOLLOUT = 0x004;
const EPoll EPOLLERR = 0x008;
const EPoll EPOLLHUP = 0x010;

union EPollData
{
    void* ptr;
    Fd fd;
    uint u32;
    ulong u64;
}

struct EPollEvent @packed
{
    EPoll events;
    EPollData data;
}

extern fn EPollFd epoll_create(CInt size);
extern fn CInt epoll_ctl(EPollFd epfd, EPollCtl op, Fd fd, EPollEvent* event);
extern fn CInt epoll_wait(EPollFd epfd, EPollEvent* events, CInt max_events, CInt timeout);
