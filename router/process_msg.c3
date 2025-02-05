module router;

import std::io;

fn bool read_short_str(char[] input, String* str, char[]* remaining_input)
{
    if (input.len == 0) return false;
    char str_len = input[0];
    input = input[1..];
    if (input.len < str_len) return false;
    *str = (String)input[:str_len];
    *remaining_input = input[str_len..];
    return true;
}

fn ServiceId Router.intern_service_name(&self, String service_name)
{
    if (self.service_name_to_id.has_key(service_name))
    {
        return self.service_name_to_id[service_name]!!;
    }

    service_name = service_name.copy();
    self.service_id_to_name.push(service_name);
    ServiceId new = (ServiceId)self.service_id_to_name.len();

    // TODO: Ensure that service name is not copied again to `HashMap`.
    self.service_name_to_id[service_name] = new;
    return new;
}

fn void Router.process_msg_login(&self, Client* client, ulong now, char[] body)
{
    if (client.state != WAITING_FOR_LOGIN)
    {
        self.mark_client_for_closing(client, "Not expected login message");
        return;
    }
    if (client.awaited_responses.written != 1)
    {
        abort("Router has inconsistent state: Client WAITING_FOR_LOGIN has corrupted awaited_responses");
    }
    AwaitedResponse awaited_response = client.awaited_responses.dequeue();
    if (awaited_response.msg_type != LOGIN)
    {
        abort("Router has inconsistent state: First awaited_response doesn't have type LOGIN");
    }
    bool header_ok =
        client.header.enqueued_by_router_us == 0 &&
        client.header.flags == 0 &&
        body.len > 0 &&
        (ulong)client.header.instance_id == 0;
    if (!header_ok)
    {
        self.mark_client_for_closing(client, "Invalid login header");
        return;
    }

    // Read login message body.
    String username, password, service_name;
    bool body_ok =
        read_short_str(body, &username, remaining_input: &body) &&
        read_short_str(body, &password, remaining_input: &body) &&
        read_short_str(body, &service_name, remaining_input: &body) &&
        body.len == 0 && // Nothing remains.
        username.len > 0 &&
        password.len > 0 &&
        service_name.len <= MAX_SERVICE_NAME_LEN;
    if (!body_ok)
    {
        self.mark_client_for_closing(client, "Invalid login body");
        return;
    }

    // TODO: Verify `username` and `password`.

    // We're done if the client is not a service.
    if (service_name.len == 0)
    {
        client.state = READY;
        io::eprintfn("Client '%s' logged in", client.addr);
        return;
    }

    // At this point client is a service.

    ServiceId service_id = self.intern_service_name(service_name);

    // Check whether the same service doesn't already exist.
    foreach (&existing_client : self.clients)
    {
        // For the purpose of detecting whether the same service is already connected,
        // we consider not only `READY` clients but also `WAITING_FOR_CLOSE` clients.
        if (existing_client.state == NOT_INITIALIZED || existing_client.state == WAITING_FOR_LOGIN) continue;
        if (existing_client.service_id == service_id)
        {
            self.mark_client_for_closing(client, "Service already connected");
            return;
        }
    }

    client.service_id = service_id;
    // We will set `client` to `READY` after adding the subscribed clients to this service.

    // Send `ADD_CLIENT_TO_SERVICE` for each existing client subscribed to this service.
    foreach (&existing_client : self.clients)
    {
        if (existing_client.state != READY) continue;

        foreach (&subscription : existing_client.subscriptions.entries[:existing_client.subscriptions.size])
        {
            if (subscription.service != service_id) continue;
            if((ulong)subscription.instance_id != 0)
            {
                abort("Router has inconsistent state: Client is connected to service which doesn't exist");
            }

            SharedPendingMessage* msg = mem::new_with_padding(SharedPendingMessage, 0);
            msg.header = {
                .enqueued_by_router_us = now,
                .msg_type = MsgType.ADD_CLIENT_TO_SERVICE.ordinal,
                .instance_id = existing_client.instance_id,
            };
            if (self.enqueue_pending_msg(client, msg))
            {
                self.enqueue_awaited_response(client, {
                    .msg_type = CLIENT_ADDED_TO_SERVICE,
                    .request_enqueued_by_router_us = now,
                    .instance_id = existing_client.instance_id,
                });
            }
            else
            {
                free(msg);
            }

            break;
        }
    }

    client.state = READY;
    io::eprintfn("Client '%s' logged in as service '%s'", client.addr, service_name);
}

fn void Router.process_msg_pong(&self, Client* client)
{
    if (client.state != READY)
    {
        self.mark_client_for_closing(client, "Not expected pong message from client in state %s", client.state);
        return;
    }
    if (client.awaited_responses.written == 0)
    {
        self.mark_client_for_closing(client, "Got pong but no response is awaited");
        return;
    }
    AwaitedResponse awaited_response = client.awaited_responses.dequeue();
    if (awaited_response.msg_type != PONG)
    {
        self.mark_client_for_closing(client, "Got pong but awaiting %s", awaited_response.msg_type);
        return;
    }
    bool header_ok =
        client.header.enqueued_by_router_us == awaited_response.request_enqueued_by_router_us &&
        client.header.flags == 0 &&
        client.header.body_size == 0 &&
        (ulong)client.header.instance_id == 0;
    if (!header_ok)
    {
        self.mark_client_for_closing(client, "Invalid pong header");
        return;
    }
}

fn void Router.process_msg(&self, Client* client)
{
    if (client.header.msg_type >= MsgType.values.len)
    {
        self.mark_client_for_closing(client, "Client sent message of unknown type %d", client.header.msg_type);
        return;
    }

    ulong now = get_monotonic_event_time_us();

    MsgType msg_type = MsgType.from_ordinal(client.header.msg_type);
    char[] body = client.body[:client.header.body_size];  // TODO: Check if this works when `body == null`.
    switch (msg_type)
    {
        case LOGIN:
            self.process_msg_login(client, now, body);
        case SUBSCRIBE_TO_SERVICE:
        case CLIENT_ADDED_TO_SERVICE:
        case CLIENT_REMOVED_FROM_SERVICE:
        case MSG_TO_ALL_CLIENTS:
        case MSG_TO_ONE_CLIENT:
        case REQUEST:
            abort("TODO: Process received message of type %s", msg_type);
        case PONG:
            self.process_msg_pong(client);

        case CONNECTED_TO_SERVICE:
        case DISCONNECTED_FROM_SERVICE:
        case ADD_CLIENT_TO_SERVICE:
        case REMOVE_CLIENT_FROM_SERVICE:
        case PING:
            self.mark_client_for_closing(client, "Client sent message of invalid type %s", msg_type);
            return;
    }

    // Free successfully processed message.
    client.received_from_header = 0;
    client.header = {};
    client.received_from_body = 0;
    free(client.body);
    client.body = null;
}
