defmodule CacheTask.Receiver do
  use GenServer

  import CacheTask.Messages
  alias :gen_tcp, as: GenTCP
  alias :queue, as: Queue
  alias CacheTask.Store
  require Logger

  @server __MODULE__
  @ip_address {127, 0, 0, 1}
  @port 12_000
  @tcp_options [mode: :binary, active: true, ip: @ip_address, packet: 4, reuseaddr: true]
  @cache_missed_reference_message cache_missed_reference_message()
  @cache_reference_ok_message cache_reference_ok_message()
  @cache_reference_message cache_reference_message()
  @cache_missed_reference_info_message cache_missed_reference_info_message()
  @cache_block_message cache_block_message()
  @spec start() :: {:ok, pid()} | :ignore | {:error, {:already_started, pid()} | term()}
  def start(), do: GenServer.start_link(@server, [@port, @tcp_options], name: @server)
  @spec stop() :: :ok
  def stop(), do: GenServer.call(@server, :stop)
  @impl true
  def init([port, tcp_options]) do
    {:ok, listen_socket} = GenTCP.listen(port, tcp_options)
    :ok = GenServer.cast(@server, :accept_connection)
    Store.init()
    Process.flag(:trap_exit, true)
    queue = Queue.new()
    start_queue()

    {:ok,
     %{
       socket: listen_socket,
       key: nil,
       queue: queue,
       buffer: <<>>,
       status: :complete,
       size: nil,
       waiting: false
     }}
  end

  @impl true
  def handle_cast(:accept_connection, %{socket: socket} = state) do
    {:ok, socket} = GenTCP.accept(socket)
    {:noreply, Map.merge(state, %{socket: socket})}
  end

  def handle_cast({:enqueue, msg}, %{queue: queue, status: status} = state) do
    status == :complete && start_queue()
    {:noreply, Map.merge(state, %{queue: Queue.in(msg, queue)})}
  end

  @impl true
  def handle_info(
        {:tcp, socket, <<@cache_missed_reference_info_message::8, size::16, data::binary>>},
        %{key: key, waiting: true} = state
      ) do
    buffer = <<data::binary>>

    state_ =
      case byte_size(buffer) == size do
        true ->
          store_data(buffer, nil)
          Map.merge(state, %{buffer: <<>>, waiting: false})

        false ->
          Map.merge(state, %{buffer: buffer, size: size, waiting: true})
      end

    {:noreply, state_}
  end

  def handle_info(
        {:tcp, socket, data},
        %{queue: queue, size: size, buffer: buffer, waiting: true} = state
      ) do
    buffer = <<buffer::binary, data::binary>>

    state_ =
      case byte_size(buffer) == size do
        true ->
          store_data(buffer, nil)
          start_queue()
          Map.merge(state, %{size: nil, buffer: <<>>})

        false ->
          Map.merge(state, %{buffer: buffer})
      end

    {:noreply, state_}
  end

  def handle_info({:tcp, socket, data}, %{queue: queue, waiting: false} = state) do
    enqueue(data)
    {:noreply, state}
  end

  def handle_info(:work, %{queue: queue, waiting: waiting} = state) do
    state =
      case Queue.is_empty(queue) do
        true ->
          Map.merge(state, %{status: :complete})

        false ->
          {{a, current}, queue} = Queue.out(queue)

          case parse_data(current, state) do
            {:ok, state} ->
              start_queue()
              Map.merge(state, %{queue: queue})

            {:error, state} ->
              queue = Queue.in_r(current, queue)
              Map.merge(state, %{queue: queue})
          end

        _ ->
          state
      end

    {:noreply, state}
  end

  def handle_info(_, state) do
    {:noreply, state}
  end

  @impl true
  def terminate(reason, _state) do

    IO.puts("#{__MODULE__}.terminate/2 called wit reason: #{inspect(reason)}")
  end

  @impl true
  def handle_call(:stop, _from, %{socket: socket} = state) do
    :ok = GenTCP.close(socket)
    Store.clear()
    {:stop, :normal, :ok, state}
  end

  def get_data() do
    #    avoid get_state
    %{key: key, status: status} = :sys.get_state(@server)

    case key do
      nil ->
        {status, <<>>}

      key ->
        {:ok, data} = Store.lookup(key)
        {status, data}
    end
  end

  def get_data(key) do
    Store.lookup(key)
  end

  def store_data(data, nil) do
    Store.save(data)
  end

  def store_data(data, key) do
    {:ok, data_} = Store.lookup(key)
    data_ = data_ <> data
    Store.save(data)
    Store.save(data_)
  end

  defp start_queue() do
    Process.send_after(self(), :work, 1)
  end

  def parse_data(
        <<@cache_block_message::8, size::16, data::binary>>,
        %{key: key, status: :complete} = state
      ) do
    buffer = <<data::binary>>

    state_ =
      case byte_size(buffer) == size do
        true ->
          {:ok, key} = store_data(buffer, key)

          Map.merge(state, %{key: key, size: nil, buffer: <<>>, status: :complete, waiting: false})

        false ->
          Map.merge(state, %{buffer: buffer, size: size, status: :incomplete, waiting: false})
      end

    {:ok, state_}
  end

  def parse_data(
        <<@cache_reference_message::8, curr_key::binary>>,
        %{key: key, socket: socket, waiting: waiting} = state
      )
      when byte_size(curr_key) == 16 do
    {:ok, data} = get_data(curr_key)
    {:ok, key} = store_data(data, key)
    !waiting && GenTCP.send(socket, <<@cache_reference_ok_message::8, curr_key::binary>>)
    {:ok, Map.merge(state, %{key: key, size: nil, buffer: <<>>, waiting: false})}
  rescue
    _ ->
      !waiting && GenTCP.send(socket, <<@cache_missed_reference_message::8, curr_key::binary>>)
      {:error, Map.merge(state, %{size: nil, buffer: <<>>, waiting: true})}
  end

  def parse_data(<<data::binary>>, %{key: key, buffer: buffer, queue: queue, size: size} = state) do
    buffer = <<buffer::binary, data::binary>>

    state_ =
      case byte_size(buffer) == size do
        true ->
          {:ok, key} = store_data(buffer, key)

          Map.merge(state, %{key: key, size: nil, buffer: <<>>, status: :complete, waiting: false})

        false ->
          Map.merge(state, %{buffer: buffer, waiting: false})
      end

    {:ok, state_}
  end

  def enqueue(msg) do
    GenServer.cast(@server, {:enqueue, msg})
  end
end
