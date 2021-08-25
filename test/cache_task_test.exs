defmodule CacheTaskTest do
  use ExUnit.Case
  doctest CacheTask

  alias CacheTask.Receiver
  alias CacheTask.Sender
  alias :crypto, as: Crypto
  alias :erlang, as: Erlang

  describe "integration" do
    setup do
      blocks =
        Enum.reduce(1..16, %{}, fn x, acc ->
          Map.put(acc, ("b" <> to_string(x)) |> String.to_atom(), Crypto.strong_rand_bytes(1_024))
        end)

      %{blocks: blocks}
    end

    test "success: clean start and stop" do
      assert {:ok, receiver_pid} = Receiver.start()
      assert {:ok, sender_pid} = Sender.start()

      assert {:complete, <<>>} = Receiver.get_data()
      assert true == Sender.is_done()

      assert :ok = Receiver.stop()
      assert :ok = Sender.stop()

      assert false == Erlang.is_process_alive(receiver_pid)
      assert false == Erlang.is_process_alive(sender_pid)
    end

    test "success: send 6 block", %{blocks: blocks} do
      assert {:ok, _} = Receiver.start()
      assert {:ok, _} = Sender.start()

      assert {:complete, <<>>} = Receiver.get_data()
      assert true == Sender.is_done()

      assert {:ok, _k1} = Sender.send_block(blocks.b1)
      assert {:ok, _k2} = Sender.send_block(blocks.b2)
      assert {:ok, _k3} = Sender.send_block(blocks.b3)
      assert {:ok, _k4} = Sender.send_block(blocks.b4)
      assert {:ok, _k5} = Sender.send_block(blocks.b5)
      assert {:ok, _k6} = Sender.send_block(blocks.b6)

      full_data =
        <<blocks.b1::binary, blocks.b2::binary, blocks.b3::binary, blocks.b4::binary,
          blocks.b5::binary, blocks.b6::binary>>

      wait_for_passing(_1_second = 1_000, fn ->
        assert {:complete, ^full_data} = Receiver.get_data()
      end)

      assert true == Sender.is_done()

      assert :ok = Receiver.stop()
      assert :ok = Sender.stop()
    end

    test "success: send 6 blocks and 3 keys", %{blocks: blocks} do
      assert {:ok, _} = Receiver.start()
      assert {:ok, _} = Sender.start()

      assert {:complete, <<>>} = Receiver.get_data()
      assert true == Sender.is_done()

      assert {:ok, _k1} = Sender.send_block(blocks.b1)
      assert {:ok, _k2} = Sender.send_block(blocks.b2)

      assert :ok = Sender.send_key(blocks.b1)

      assert {:ok, _k3} = Sender.send_block(blocks.b3)
      assert {:ok, _k4} = Sender.send_block(blocks.b4)

      assert :ok = Sender.send_key(blocks.b2)

      assert {:ok, _k5} = Sender.send_block(blocks.b5)
      assert {:ok, _k6} = Sender.send_block(blocks.b6)

      assert :ok = Sender.send_key(blocks.b1)

      full_data =
        <<blocks.b1::binary, blocks.b2::binary, blocks.b1::binary, blocks.b3::binary,
          blocks.b4::binary, blocks.b2::binary, blocks.b5::binary, blocks.b6::binary,
          blocks.b1::binary>>

      wait_for_passing(_1_second = 1_000, fn ->
        assert {:complete, ^full_data} = Receiver.get_data()
      end)

      assert true == Sender.is_done()

      assert :ok = Receiver.stop()
      assert :ok = Sender.stop()
    end

    test "success: send 16 blocks and 4 keys", %{blocks: blocks} do
      assert {:ok, _} = Receiver.start()
      assert {:ok, _} = Sender.start()

      assert {:complete, <<>>} = Receiver.get_data()
      assert true == Sender.is_done()

      assert {:ok, _k1} = Sender.send_block(blocks.b1)
      assert {:ok, _k2} = Sender.send_block(blocks.b2)

      assert :ok = Sender.send_key(blocks.b1)

      assert {:ok, _k3} = Sender.send_block(blocks.b3)
      assert {:ok, _k4} = Sender.send_block(blocks.b4)

      assert :ok = Sender.send_key(blocks.b2)

      assert {:ok, _k5} = Sender.send_block(blocks.b5)
      assert {:ok, _k6} = Sender.send_block(blocks.b6)

      assert :ok = Sender.send_key(blocks.b1)

      assert {:ok, _k7} = Sender.send_block(blocks.b7)
      assert {:ok, _k8} = Sender.send_block(blocks.b8)
      assert {:ok, _k9} = Sender.send_block(blocks.b9)
      assert {:ok, _k10} = Sender.send_block(blocks.b10)
      assert {:ok, _k11} = Sender.send_block(blocks.b11)
      assert {:ok, _k12} = Sender.send_block(blocks.b12)
      assert {:ok, _k13} = Sender.send_block(blocks.b13)

      assert :ok = Sender.send_key(blocks.b1)

      assert {:ok, _k14} = Sender.send_block(blocks.b14)
      assert {:ok, _k15} = Sender.send_block(blocks.b15)
      assert {:ok, _k16} = Sender.send_block(blocks.b16)

      full_data =
        <<blocks.b1::binary, blocks.b2::binary, blocks.b1::binary, blocks.b3::binary,
          blocks.b4::binary, blocks.b2::binary, blocks.b5::binary, blocks.b6::binary,
          blocks.b1::binary, blocks.b7::binary, blocks.b8::binary, blocks.b9::binary,
          blocks.b10::binary, blocks.b11::binary, blocks.b12::binary, blocks.b13::binary,
          blocks.b1::binary, blocks.b14::binary, blocks.b15::binary, blocks.b16::binary>>

      wait_for_passing(_1_second = 1_000, fn ->
        assert {:complete, ^full_data} = Receiver.get_data()
      end)

      assert true == Sender.is_done()

      assert :ok = Receiver.stop()
      assert :ok = Sender.stop()
    end
  end

  defp wait_for_passing(timeout, fun) when timeout > 0 do
    fun.()
  rescue
    _ ->
      Process.sleep(1)
      wait_for_passing(timeout - 1, fun)
  end

  defp wait_for_passing(_timeout, fun), do: fun.()
end
