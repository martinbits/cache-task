defmodule CacheTask.StoreTest do
  use ExUnit.Case
  doctest CacheTask.Store

  alias CacheTask.Store
  alias :crypto, as: Crypto

  describe "CacheTask.Store API" do
    setup do
      blocks =
        Enum.reduce(1..11, %{}, fn x, acc ->
          Map.put(acc, ("b" <> to_string(x)) |> String.to_atom(), Crypto.strong_rand_bytes(100))
        end)

      %{blocks: blocks}
    end

    test "success: verify it is working well", %{blocks: blocks} do
      assert :ok = Store.init()
      assert :ok = Store.clear()

      assert {:ok, k1} = Store.save(blocks.b1)
      assert {:ok, blocks.b1} == Store.lookup(k1)

      assert {:ok, k2} = Store.save(blocks.b2)
      assert {:ok, blocks.b2} == Store.lookup(k2)

      assert {:ok, k3} = Store.save(blocks.b3)
      assert {:ok, _k4} = Store.save(blocks.b4)
      assert {:ok, _k5} = Store.save(blocks.b5)
      assert {:ok, _k6} = Store.save(blocks.b6)
      assert {:ok, _k7} = Store.save(blocks.b7)
      assert {:ok, _k8} = Store.save(blocks.b8)
      assert {:ok, _k9} = Store.save(blocks.b9)

      assert {:ok, blocks.b1} == Store.lookup(k1)
      assert {:ok, blocks.b2} == Store.lookup(k2)

      assert {:ok, k10} = Store.save(blocks.b10)
      assert {:ok, blocks.b10} == Store.lookup(k10)

      assert :not_found = Store.lookup(k1)
      assert {:ok, blocks.b2} == Store.lookup(k2)
      assert {:ok, blocks.b3} == Store.lookup(k3)

      assert {:ok, _k11} = Store.save(blocks.b11)

      assert :not_found = Store.lookup(k1)
      assert :not_found = Store.lookup(k2)
      assert {:ok, blocks.b3} == Store.lookup(k3)

      assert :ok = Store.clear()
      assert :ok = Store.close()
    end
  end
end
