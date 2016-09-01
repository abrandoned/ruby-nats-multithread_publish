unless defined?(NATS)
  require "nats/client"
end

require "concurrent"

module NATS
  def initialize(options)
    @options = options
    process_uri_options

    @buf = nil
    @ssid, @subs = 1, {}
    @err_cb = NATS.err_cb
    @close_cb = NATS.close_cb
    @reconnect_cb = NATS.reconnect_cb
    @disconnect_cb = NATS.disconnect_cb
    @reconnect_timer, @needed = nil, nil
    @connected, @closing, @reconnecting, @conn_cb_called = false, false, false, false
    @msgs_received = @msgs_sent = @bytes_received = @bytes_sent = @pings = 0
    @pending = ::Concurrent::Map.new
    @pending_size = ::Concurrent::AtomicFixnum.new(0)
    @server_info = { }

    # Mark whether we should be connecting securely, try best effort
    # in being compatible with present ssl support.
    @ssl = false
    @tls = nil
    @tls = options[:tls] if options[:tls]
    @ssl = options[:ssl] if options[:ssl] or @tls

    send_connect_command
  end

  def flush_pending #:nodoc:
    pending = @pending.get_and_set(:pending, "")
    sub_commands = @pending.get_and_set(:subs, "")

    send_data(sub_commands) if sub_commands && !sub_commands.empty?
    send_data(pending) if pending && !pending.empty?

    @pending_size.decrement(@pending_size.value)
  end

  def pending_data_size
    get_outbound_data_size + @pending_size.value
  end

  def process_connect #:nodoc:
    # Reset reconnect attempts since TCP connection has been successful at this point.
    current = server_pool.first
    current[:was_connected] = true
    current[:reconnect_attempts] ||= 0
    cancel_reconnect_timer if reconnecting?

    # Whip through any pending SUB commands since we replay
    # all subscriptions already done anyway.
    @pending.get_and_set(:subs, "")
    @subs.each_pair { |k, v| send_command("SUB #{v[:subject]} #{v[:queue]} #{k}#{CR_LF}") }

    unless user_err_cb? or reconnecting?
      @err_cb = proc { |e| raise e }
    end

    # We have validated the connection at this point so send CONNECT
    # and any other pending commands which we need to the server.
    flush_pending

    if (connect_cb and not @conn_cb_called)
      # We will round trip the server here to make sure all state from any pending commands
      # has been processed before calling the connect callback.
      queue_server_rt do
        connect_cb.call(self)
        @conn_cb_called = true
      end
    end

    # Notify via reconnect callback that we are again plugged again into the system.
    if reconnecting?
      @reconnecting = false
      @reconnect_cb.call(self) unless @reconnect_cb.nil?
    end

    # Initialize ping timer and processing
    @pings_outstanding = 0
    @pongs_received = 0
    @ping_timer = EM.add_periodic_timer(@options[:ping_interval]) do
      send_ping
    end
  end

  def unbind #:nodoc:
    # Allow notifying from which server we were disconnected,
    # but only when we didn't trigger disconnecting ourselves.
    if @disconnect_cb and connected? and not closing?
      disconnect_cb.call(NATS::ConnectError.new(disconnect_error_string))
    end

    # If we are closing or shouldn't reconnect, go ahead and disconnect.
    process_disconnect and return if (closing? or should_not_reconnect?)
    @reconnecting = true if connected?
    @connected = false
    @pending.clear
    @pongs = nil
    @buf = nil
    cancel_ping_timer

    schedule_primary_and_connect
  end

  def send_command(command, priority = false) #:nodoc:
    was_empty = false

    if command[0..2] == SUB_OP
      @pending.compute(:subs) do |val|
        "#{val}#{command}"
      end
    else
      @pending.compute(:pending) do |val|
        was_empty = val.empty?
        priority ? "#{command}#{val}" : "#{val}#{command}"
      end
    end

    @pending_size.increment(command.bytesize)

    if connected? && (was_empty || @pending_size.value > MAX_PENDING_SIZE)
      EM.next_tick { flush_pending }
    end

    if (@options[:fast_producer_error] && pending_data_size > FAST_PRODUCER_THRESHOLD)
      err_cb.call(NATS::ClientError.new("Fast Producer: #{pending_data_size} bytes outstanding"))
    end
    true
  end
end
