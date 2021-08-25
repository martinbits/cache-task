defmodule CacheTask.Sender do
  use GenServer

  import CacheTask.Messages

  defmodule State do
    @moduledoc false

    defstruct socket: nil,
              send_window: [],
              buffer: <<>>
  end

  alias CacheTask.Sender.State
  alias :crypto, as: Crypto
  alias :gen_tcp, as: GenTCP
  alias :inet, as: Inet

  @server __MODULE__
  @ip_address '127.0.0.1'
  @port 12_000
  @tcp_options [{:active, false}, {:mode, :binary}, {:packet, 4}]

  @cache_missed_reference_message cache_missed_reference_message()
  @cache_reference_ok_message cache_reference_ok_message()

  @spec start() :: {:ok, pid()} | :ignore | {:error, {:already_started, pid()} | term()}
  def start(), do: GenServer.start_link(@server, [], name: @server)

  @spec stop() :: :ok
  def stop(), do: GenServer.call(@server, :stop)

  @spec is_done() :: boolean()
  def is_done(), do: GenServer.call(@server, :is_done)

  @spec send_raw(data :: binary()) :: :ok
  def send_raw(data) when is_binary(data), do: GenServer.cast(@server, {:send_raw, data})

  @spec send_block(data :: binary()) :: {:ok, binary()}
  def send_block(data) when is_binary(data) do
    :ok = GenServer.cast(@server, {:send_block, data})
    {:ok, Crypto.hash(:md5, data)}
  end

  @spec send_key(data :: binary()) :: :ok
  def send_key(data) when is_binary(data) do
    key = Crypto.hash(:md5, data)
    GenServer.cast(@server, {:send_key, key, data})
  end

  # Callbacks

  @impl true
  def init([]) do
    {:ok, socket} = GenTCP.connect(@ip_address, @port, @tcp_options)
    :ok = Inet.setopts(socket, [{:active, :once}])
    {:ok, %State{socket: socket}}
  end

  @impl true
  def handle_call(:is_done, _from, %State{send_window: []} = state) do
    {:reply, true, state}
  end

  def handle_call(:is_done, _from, state) do
    {:reply, false, state}
  end

  def handle_call(:stop, _from, %State{socket: socket} = state) do
    :ok = GenTCP.close(socket)
    {:stop, :normal, :ok, state}
  end

  @impl true
  def handle_cast({:send_raw, data}, %State{socket: socket} = state) do
    :ok = gen_tcp_send(socket, raw_message(data))
    {:noreply, state}
  end

  def handle_cast({:send_block, data}, %State{socket: socket} = state) do
    :ok = gen_tcp_send(socket, block_message(data))
    {:noreply, state}
  end

  def handle_cast(
        {:send_key, key, data},
        %State{socket: socket, send_window: send_window} = state
      ) do
    :ok = gen_tcp_send(socket, reference_message(key))
    {:noreply, %State{state | send_window: p_inc(key, data, send_window)}}
  end

  @impl true
  def handle_info(
        {:tcp, socket, data},
        %State{socket: socket, send_window: send_window, buffer: buffer} = state
      ) do
    :ok = Inet.setopts(socket, [{:active, :once}])
    {buffer, send_window} = reference_ack(socket, <<buffer::binary, data::binary>>, send_window)
    {:noreply, %State{state | send_window: send_window, buffer: buffer}}
  end

  def handle_info(_, state) do
    {:noreply, state}
  end

  @impl true
  def terminate(_, _), do: :ok

  defp gen_tcp_send(socket, <<data::256*8, rest::binary>>) do
    GenTCP.send(socket, <<data::256*8>>)
    gen_tcp_send(socket, rest)
  end

  defp gen_tcp_send(socket, data) do
    GenTCP.send(socket, data)
  end

  defp p_inc(key, data, send_window) do
    case List.keytake(send_window, key, 0) do
      nil -> [{key, 1, data} | send_window]
      {{^key, count, ^data}, send_window} -> [{key, count + 1, data} | send_window]
    end
  end

  defp p_dec(_key, 1, _data, send_window), do: send_window
  defp p_dec(key, count, data, send_window), do: [{key, count - 1, data} | send_window]

  defp reference_ack(
         socket,
         <<@cache_missed_reference_message::8, key::16*8, rest::binary>>,
         send_window
       ) do
    key = <<key::16*8>>
    {{^key, count, data}, send_window} = List.keytake(send_window, key, 0)
    :ok = gen_tcp_send(socket, missed_reference_info_message(data))
    send_window = p_dec(key, count, data, send_window)
    reference_ack(socket, rest, send_window)
  end

  defp reference_ack(
         socket,
         <<@cache_reference_ok_message::8, key::16*8, rest::binary>>,
         send_window
       ) do
    key = <<key::16*8>>
    {{^key, count, data}, send_window} = List.keytake(send_window, key, 0)
    send_window = p_dec(key, count, data, send_window)
    reference_ack(socket, rest, send_window)
  end

  defp reference_ack(_socket, buffer, send_window) when is_binary(buffer) do
    {buffer, send_window}
  end
end
