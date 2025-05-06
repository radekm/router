module router;

import std::collections;
import std::io;
import libc;
import btree;

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
    BTreeSlot{ServiceId} slot = self.service_name_to_id.@get_slot(
        fn int(List{String}* service_id_to_name, String service_name, ServiceId item_from_tree)
        {
            String existing_service_name = service_id_to_name.get((usz)item_from_tree - 1);
            if (service_name.len < existing_service_name.len) return -1;
            if (service_name.len > existing_service_name.len) return 1;
            return libc::memcmp(service_name.ptr, existing_service_name.ptr, service_name.len);
        }, &self.service_id_to_name, service_name);
    if (slot.existed) return *slot.item;

    if (self.service_id_to_name.len() == MAX_SERVICE_NAMES) abort("Too many interned service names");

    service_name = service_name.copy(mem);  // NOTE: We don't bother freeing this, it's freed by OS when program ends.
    self.service_id_to_name.push(service_name);
    ServiceId new = (ServiceId)self.service_id_to_name.len();
    *slot.item = new;

    return new;
}

fn Subscription*? Router.find_subscription_by_service_id(&self, Client* client, ServiceId service_id)
{
    foreach (&subscription : client.subscriptions.entries[:client.subscriptions.size])
    {
        if (subscription.service == service_id) return subscription;
    }
    return NOT_FOUND?;
}

fn Subscription*? Router.find_subscription_by_instance_id(&self, Client* client, ClientInstanceId instance_id)
{
    foreach (&subscription : client.subscriptions.entries[:client.subscriptions.size])
    {
        if (subscription.instance_id == instance_id) return subscription;
    }
    return NOT_FOUND?;
}

fn bool Router.enqueue_msg_and_response_add_client_to_service(&self, ulong now, Client* service, ClientInstanceId client)
{
    SharedPendingMessage* msg = mem::new_with_padding(SharedPendingMessage, 0);
    msg.header = {
        .enqueued_by_router_us = now,
        .msg_type = MsgType.ADD_CLIENT_TO_SERVICE.ordinal,
        .instance_id = client,
    };
    if (self.enqueue_pending_msg(service, msg))
    {
        return self.enqueue_awaited_response(service, {
            .msg_type = CLIENT_ADDED_TO_SERVICE,
            .request_enqueued_by_router_us = now,
            .instance_id = client,
        });
    }
    else
    {
        free(msg);
        return false;
    }
}

fn void Router.enqueue_msg_and_response_remove_client_from_service(&self, ulong now, Client* service, ClientInstanceId client)
{
    SharedPendingMessage* msg = mem::new_with_padding(SharedPendingMessage, 0);
    msg.header = {
        .enqueued_by_router_us = now,
        .msg_type = MsgType.REMOVE_CLIENT_FROM_SERVICE.ordinal,
        .instance_id = client,
    };
    if (self.enqueue_pending_msg(service, msg))
    {
        self.enqueue_awaited_response(service, {
            .msg_type = CLIENT_REMOVED_FROM_SERVICE,
            .request_enqueued_by_router_us = now,
            .instance_id = client,
        });
    }
    else
    {
        free(msg);
    }
}

fn bool Router.check_if_message_is_expected_and_header(&self, Client* client, MsgType msg_type)
{
    // Check client state and whether it's a service.
    switch (msg_type)
    {
        case LOGIN:
            if (client.state != WAITING_FOR_LOGIN)
            {
                self.mark_client_for_closing(client, "Not expected %s message", msg_type);
                return false;
            }

        // Only services can send these messages.
        case CLIENT_ADDED_TO_SERVICE:
        case CLIENT_REMOVED_FROM_SERVICE:
        case MSG_TO_ALL_CLIENTS:
        case MSG_TO_ONE_CLIENT:
            if (client.state != READY || client.service_id == 0)
            {
                self.mark_client_for_closing(client, "Not expected %s message", msg_type);
                return false;
            }

        // Any client can send these messages.
        case SUBSCRIBE_TO_SERVICE:
        case REQUEST:
        case PONG:
            if (client.state != READY)
            {
                self.mark_client_for_closing(client, "Not expected %s message", msg_type);
                return false;
            }

        // The following messages can only be sent by router.
        case CONNECTED_TO_SERVICE:
        case DISCONNECTED_FROM_SERVICE:
        case ADD_CLIENT_TO_SERVICE:
        case REMOVE_CLIENT_FROM_SERVICE:
        case PING:
            self.mark_client_for_closing(client, "Client sent message of invalid type %s", msg_type);
            return false;
    }

    // Check header. If desirable check it against awaited response.
    switch (msg_type)
    {
        // Check against awaited response.
        case LOGIN:
        case CLIENT_ADDED_TO_SERVICE:
        case CLIENT_REMOVED_FROM_SERVICE:
        case PONG:
            if (client.awaited_responses.written == 0)
            {
                self.mark_client_for_closing(client, "Got %s but no response is awaited", msg_type);
                return false;
            }
            AwaitedResponse awaited_response = client.awaited_responses.dequeue();
            if (awaited_response.msg_type != msg_type)
            {
                self.mark_client_for_closing(client, "Got %s but awaiting %s", msg_type, awaited_response.msg_type);
                return false;
            }

            // Check if data from the header match data from the awaited response.
            bool header_ok = true;
            header_ok = header_ok && client.header.enqueued_by_router_us ==
                (msg_type == PONG ? awaited_response.request_enqueued_by_router_us : 0);
            header_ok = header_ok && client.header.flags == 0;
            header_ok = header_ok &&
                (msg_type == LOGIN ? client.header.body_size > 0 : client.header.body_size == 0);
            if (msg_type == CLIENT_ADDED_TO_SERVICE || msg_type == CLIENT_REMOVED_FROM_SERVICE)
            {
                // NOTE: `process_msg_*` function must still verify whether the client still exists.
                header_ok = header_ok && client.header.instance_id == awaited_response.instance_id;
            }
            else
            {
                header_ok = header_ok && (ulong)client.header.instance_id == 0;
            }

            if (!header_ok)
            {
                self.mark_client_for_closing(client, "Invalid %s header", msg_type);
                return false;
            }

        case MSG_TO_ALL_CLIENTS:
        case MSG_TO_ONE_CLIENT:
        case SUBSCRIBE_TO_SERVICE:
        case REQUEST:
            bool header_ok = true;
            header_ok = header_ok && client.header.enqueued_by_router_us == 0;
            header_ok = header_ok && client.header.flags == 0;
            // Empty body of message (`MSG_TO_ALL_CLIENTS`, `MSG_TO_ONE_CLIENT`, `REQUEST`) doesn't make sense.
            header_ok = header_ok &&  client.header.body_size > 0;
            if (msg_type == MSG_TO_ONE_CLIENT || msg_type == REQUEST)
            {
                // Concrete client or service must be specified.
                // NOTE: `process_msg_*` function must still verify whether the client still exists
                //       and in case of `REQUEST` whether the client is a service.
                header_ok = header_ok && (ulong)client.header.instance_id != 0;
            }
            else
            {
                header_ok = header_ok && (ulong)client.header.instance_id == 0;
            }

            if (!header_ok)
            {
                self.mark_client_for_closing(client, "Invalid %s header", msg_type);
                return false;
            }

        default:
    }

    return true;
}

fn void Router.process_msg_login(&self, Client* client, ulong now, char[] body)
{
    if (client.awaited_responses.written != 0)
    {
        abort("Router has inconsistent state: Client WAITING_FOR_LOGIN had more than one awaited response");
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
        io::eprintfn("Client logged in (instance %d, addr '%s')", (ulong)client.instance_id, client.addr);
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

        if (try subscription = self.find_subscription_by_service_id(existing_client, service_id))
        {
            if((ulong)subscription.instance_id != 0)
            {
                abort("Router has inconsistent state: Client is connected to service which doesn't exist");
            }

            // Both queues `pending_messages` and `awaited_responses` should have enough space
            // because the service has just started.
            if (!self.enqueue_msg_and_response_add_client_to_service(now, client, existing_client.instance_id))
            {
                abort("Router has inconsistent state: Queues don't have enough space after login");
            }
        }
    }

    client.state = READY;
    io::eprintfn(
        "Client logged in as service (instance %d, addr '%s', service: '%s')",
        (ulong)client.instance_id,
        client.addr,
        service_name);
}

fn void Router.process_msg_subscribe_to_service(&self, Client* client, ulong now, char[] body)
{
    if (client.subscriptions.size == MAX_SUBSCRIPTIONS)
    {
        self.mark_client_for_closing(client, "Too many subscriptions");
        return;
    }
    bool header_ok =
        client.header.enqueued_by_router_us == 0 &&
        client.header.flags == 0 &&
        body.len > 0 &&
        (ulong)client.header.instance_id == 0;
    if (!header_ok)
    {
        self.mark_client_for_closing(client, "Invalid SUBSCRIBE_TO_SERVICE header");
        return;
    }

    // Read subscribe to service message body.
    String service_name;
    bool body_ok =
        read_short_str(body, &service_name, remaining_input: &body) &&
        body.len == 0 && // Nothing remains.
        service_name.len > 0 &&
        service_name.len <= MAX_SERVICE_NAME_LEN;
    if (!body_ok)
    {
        self.mark_client_for_closing(client, "Invalid SUBSCRIBE_TO_SERVICE body");
        return;
    }

    ServiceId service_id = self.intern_service_name(service_name);

    // Ensure that `client` is not subscribed to this service.
    foreach (&subscription : client.subscriptions.entries[:client.subscriptions.size])
    {
        if (subscription.service == service_id)
        {
            self.mark_client_for_closing(client, "Client already subscribed to service '%s'", service_name);
            return;
        }
    }

    // Add subscription.
    client.subscriptions.push_try({ .service = service_id })!!;

    // If the service exists, then send `ADD_CLIENT_TO_SERVICE` to it.
    foreach (&existing_client : self.clients)
    {
        if (existing_client.state != READY) continue;
        if (existing_client.service_id != service_id) continue;

        self.enqueue_msg_and_response_add_client_to_service(now, existing_client, client.instance_id);

        break;
    }
}

fn void Router.process_msg_client_added_to_service(&self, Client* service, ulong now, char[] body)
{
    Client* client = &self.clients[service.header.instance_id.idx];

    // Client either doesn't exist anymore or it's `WAITING_FOR_CLOSE`.
    if (client.state != READY || client.instance_id != service.header.instance_id)
    {
        // Ask the service to remove the client.
        self.enqueue_msg_and_response_remove_client_from_service(now, service, service.header.instance_id);
        return;
    }

    // At this point the client is subscribed.

    // Add the new subscriber to the service.
    service.subscribers.push_try(client.instance_id)!!;

    // Add service instance id to the subscription.
    if (try subscription = self.find_subscription_by_service_id(client, service.service_id))
    {
        if ((ulong)subscription.instance_id != 0)
        {
            abort("Router has inconsistent state: Subscription to service '%s' contains instance id %d",
                self.get_service_name(service.service_id), (ulong)subscription.instance_id);
        }
        subscription.instance_id = service.instance_id;
    }
    else
    {
        abort("Router has inconsistent state: Missing subscription to service '%s'",
            self.get_service_name(service.service_id));
    }

    // Enqueue notification `CONNECTED_TO_SERVICE` to the client.
    String service_name = self.get_service_name(service.service_id);
    uint body_size = 1 + service_name.len;
    SharedPendingMessage* msg = mem::new_with_padding(SharedPendingMessage, body_size);
    msg.header = {
        .enqueued_by_router_us = now,
        .msg_type = MsgType.CONNECTED_TO_SERVICE.ordinal,
        .body_size = body_size,
        .instance_id = service.instance_id,
    };
    msg.body[0] = (char)service_name.len;
    mem::copy(&msg.body[1], &service_name[0], service_name.len);
    if (!self.enqueue_pending_msg(client, msg))
    {
        free(msg);
    }
}

fn void Router.process_msg_client_removed_from_service(&self, Client* service, ulong now, char[] body)
{
    // Clients can't unsubcribe. So the only situation when router sends
    // `REMOVE_CLIENT_FROM_SERVICE` and awaits response `CLIENT_REMOVED_FROM_SERVICE`
    // is when a client socket is closed. So in this case we don't have to notify the client
    // because it doesn't exist when `CLIENT_REMOVED_FROM_SERVICE` is received.

    // Check that the client really doesn't exist.
    Client* client = &self.clients[service.header.instance_id.idx];
    if (client.instance_id == service.header.instance_id)
    {
        abort("Got CLIENT_REMOVED_FROM_SERVICE while client still exists");
    }

    // Because we removed the client from the subscribers
    // of the service when enqueuing `REMOVE_CLIENT_FROM_SERVICE`
    // we don't need to do it here.
}

fn void Router.process_msg_to_all_clients(&self, Client* service, ulong now, char[] body)
{
    SharedPendingMessage* msg = mem::new_with_padding(SharedPendingMessage, body.len);
    msg.header = {
        .enqueued_by_router_us = now,
        .msg_type = MsgType.MSG_TO_ALL_CLIENTS.ordinal,
        .body_size = body.len,
        .instance_id = service.instance_id,
    };
    msg.body[:body.len] = body[..];
    foreach (ClientInstanceId subscriber : service.subscribers.entries[:service.subscribers.size])
    {
        Client* subscriber_client = &self.clients[subscriber.idx];

        if (subscriber_client.instance_id != subscriber)
        {
            abort("Router has inconsistent state: Subscriber doesn't exist");
        }
        if (subscriber_client.state == WAITING_FOR_CLOSE) continue;  // Subscriber is marked for closing.
        if (subscriber_client.state != READY) abort("Subscriber has invalid state");

        self.enqueue_pending_msg(subscriber_client, msg);
    }
    if (msg.ref_count == 0)
    {
        free(msg);
    }
}

fn void Router.process_msg_to_one_client(&self, Client* service, ulong now, char[] body)
{
    // CONSIDER: Instead of searching among subscribers we could go into the client
    //           and check its subscriptions. This could be faster
    //           because there could be `MAX_CLIENTS` subscribers
    //           but only `MAX_SUBSCRIPTIONS` subscriptions.

    // It may happen that the subscriber doesn't exist.
    // If the subscriber is closed router sends `REMOVE_CLIENT_FROM_SERVICE`.
    // But before the service processes `REMOVE_CLIENT_FROM_SERVICE`,
    // it may send `MSG_TO_ONE_CLIENT` to router.
    // This scenario is ok. We just ignore the message.
    if (!service.subscribers.contains(service.header.instance_id)) return;

    Client* subscriber_client = &self.clients[service.header.instance_id.idx];
    if (subscriber_client.instance_id != service.header.instance_id)
    {
        abort("Router has inconsistent state: Subscriber doesn't exist");
    }
    if (subscriber_client.state == WAITING_FOR_CLOSE) return;  // Subscriber is marked for closing.
    if (subscriber_client.state != READY) abort("Subscriber has invalid state");

    // We have found the subscriber.
    SharedPendingMessage* msg = mem::new_with_padding(SharedPendingMessage, body.len);
    msg.header = {
        .enqueued_by_router_us = now,
        .msg_type = MsgType.MSG_TO_ONE_CLIENT.ordinal,
        .body_size = body.len,
        .instance_id = service.instance_id,
    };
    msg.body[:body.len] = body[..];
    if (!self.enqueue_pending_msg(subscriber_client, msg))
    {
        free(msg);
    }
}

fn void Router.process_msg_request(&self, Client* client, ulong now, char[] body)
{
    // Check that the client is subscribed to service.
    // Currently we don't know service id so we search subscriptions by its instance id.
    Subscription*? subscription = self.find_subscription_by_instance_id(client, client.header.instance_id);
    // It's ok if the subscription doesn't exist.
    if (catch subscription) return;

    Client* service = &self.clients[client.header.instance_id.idx];
    if (service.instance_id != subscription.instance_id || service.service_id != subscription.service)
    {
        abort("Router has inconsistent state: Values from Subscription don't match values from Client");
    }
    if (service.state == WAITING_FOR_CLOSE) return;  // Service is marked for closing.
    if (service.state != READY) abort("Service has invalid state");

    // We have found the service.
    SharedPendingMessage* msg = mem::new_with_padding(SharedPendingMessage, body.len);
    msg.header = {
        .enqueued_by_router_us = now,
        .msg_type = MsgType.REQUEST.ordinal,
        .body_size = body.len,
        .instance_id = client.instance_id,
    };
    msg.body[:body.len] = body[..];
    if (!self.enqueue_pending_msg(service, msg))
    {
        free(msg);
    }
}

fn void Router.process_msg_pong(&self, Client* client)
{
    // Nothing to do.
}

fn void Router.process_msg(&self, Client* client)
{
    if (client.header.msg_type >= MsgType.values.len)
    {
        self.mark_client_for_closing(client, "Client sent message of unknown type %d", client.header.msg_type);
        return;
    }
    MsgType msg_type = MsgType.from_ordinal(client.header.msg_type);

    if (!self.check_if_message_is_expected_and_header(client, msg_type)) return;

    ulong now = get_monotonic_event_time_us();
    char[] body = client.body[:client.header.body_size];
    switch (msg_type)
    {
        case LOGIN:
            self.process_msg_login(client, now, body);
        case SUBSCRIBE_TO_SERVICE:
            self.process_msg_subscribe_to_service(client, now, body);
        case CLIENT_ADDED_TO_SERVICE:
            self.process_msg_client_added_to_service(client, now, body);
        case CLIENT_REMOVED_FROM_SERVICE:
            self.process_msg_client_removed_from_service(client, now, body);
        case MSG_TO_ALL_CLIENTS:
            self.process_msg_to_all_clients(client, now, body);
        case MSG_TO_ONE_CLIENT:
            self.process_msg_to_one_client(client, now, body);
        case REQUEST:
            self.process_msg_request(client, now, body);
        case PONG:
            self.process_msg_pong(client);

        case CONNECTED_TO_SERVICE:
        case DISCONNECTED_FROM_SERVICE:
        case ADD_CLIENT_TO_SERVICE:
        case REMOVE_CLIENT_FROM_SERVICE:
        case PING:
            abort("check_if_message_is_expected_and_header is not working");
    }

    // Free successfully processed message.
    client.received_from_header = 0;
    client.header = {};
    client.received_from_body = 0;
    free(client.body);
    client.body = null;
}
