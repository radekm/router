module router;

import std::collections;
import std::encoding::json;
import std::io;
import std::net::os;
import libc;

macro void abort_with_errno(String string, ...) @noreturn
{
    CInt errno = libc::errno();
	io::eprintfn("Last errno %d: %s", errno, libc::strerror(errno));
    abort(string, $vasplat);
}

fn void Config.destroy(&self)
{
    free(self.host);
    *self = {};
}

fn Config parse_config()
{
    @pool()
    {
        // Unfortunately shorter variant doesn't compile:
        // char[] bytes = file::load_temp("config.json") ?? abort("Unable to open config.json file");
        // See https://github.com/c3lang/c3c/issues/1913.
        char[]! bytes = file::load_temp("config.json");
        if (catch bytes) abort("Unable to open config.json file");
        Object*! obj = json::temp_parse_string((String)bytes);
        if (catch obj) abort("Unable to parse config.json file");

        String! host = obj.get_string("host");
        if (catch host) abort("Unable to read host");
        int! port = obj.get_int("port");
        if (catch port) abort("Unable to read port");

        Config config;
        config.host = host.zstr_copy();
        config.port = port;

        return config;
    };
}

fn void main()
{
    Config config = parse_config();
    defer config.destroy();

    // Ignore SIGPIPE to prevent `send` from generating SIGPIPE if the peer
    // on a stream-oriented socket has closed the connection.
    // Alternatively we can pass flag `MSG_NOSIGNAL` to every `send` call.
    if (libc::signal(libc::SIGPIPE, SIG_IGN) == SIG_ERR) abort_with_errno("Failed to ignore SIGPIPE");

    AddrInfo hints;
    hints.ai_family = os::AF_INET;
    hints.ai_socktype = os::SOCK_STREAM;
    hints.ai_flags = os::AI_PASSIVE;
    AddrInfo* ai;

    @stack_mem(128; Allocator allocator)
    {
        DString port_str;
        port_str.new_init(allocator: allocator);
        port_str.appendf("%d", config.port);

        CInt error = os::getaddrinfo(config.host, port_str.zstr_view(), &hints, &ai);
        if (error == EAI_SYSTEM)
        {
            // `EAI_SYSTEM` means that error is in `errno`.
            abort_with_errno("getaddrinfo failed for host '%s' and port '%s'", config.host, port_str);
        }
        else if (error != 0)
        {
        	io::eprintfn("Error %d from getaddrinfo: %s", error, gai_strerror(error));
            abort("getaddrinfo failed for host '%s' and port '%s'", config.host, port_str);
        }
    };
    defer os::freeaddrinfo(ai);

    if (ai == null) abort("getaddrinfo returned nothing");
    if (ai.ai_next != null) abort("getaddrinfo returned more than one address");

    NativeSocket server_socket = os::socket(os::AF_INET, os::SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (server_socket == -1) abort_with_errno("Server socket wasn't created");
    defer (void)server_socket.close();

    CInt optval = 1;
    if (os::setsockopt(server_socket, os::SOL_SOCKET, os::SO_REUSEADDR, &optval, $sizeof(optval)) != 0)
    {
        abort_with_errno("setsockopt failed when setting SO_REUSEADDR");
    }

    if (os::bind(server_socket, ai.ai_addr, ai.ai_addrlen) == -1) abort_with_errno("bind failed");

    if (os::listen(server_socket, backlog: 32) == -1) abort_with_errno("listen failed");

    EPollFd epfd = epoll_create(1);
    if (epfd == -1) abort_with_errno("epoll_create failed");
    defer epfd.close();

    // TODO: Start server loop.
}
