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

fn void Router.process_msg(&self, Client* client)
{
    if (client.header.msg_type >= MsgType.values.len)
    {
        self.mark_client_for_closing(client, "Client sent message of unknown type %d", client.header.msg_type);
        return;
    }

    MsgType msg_type = MsgType.from_ordinal(client.header.msg_type);
    char[] body = client.body[:client.header.body_size];  // TODO: funguje ot pro null? overit!
    switch (msg_type)
    {
        case LOGIN:
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
                self.mark_client_for_closing(client, "Invalid %s header", msg_type);
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
            // TODO: Intern service name and set it to `client`.
            // TODO: If the client is a service then subscribe clients waiting for this service.

            client.state = READY;

        case SUBSCRIBE_TO_SERVICE:
        case CLIENT_ADDED_TO_SERVICE:
        case CLIENT_REMOVED_FROM_SERVICE:
        case MSG_TO_ALL_CLIENTS:
        case MSG_TO_ONE_CLIENT:
        case REQUEST:
        case PONG:
            abort("TODO: Process received message of type %s", msg_type);

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
