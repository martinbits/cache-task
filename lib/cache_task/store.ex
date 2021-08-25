defmodule CacheTask.Store do
  @moduledoc false
  @name 'cache.dets'

  alias :crypto, as: Crypto
  alias :dets, as: Dets

  @spec init() :: :ok
  def init do
    if Enum.member?(Dets.all(), @name) do
      :ok
    else
      {:ok, @name} = Dets.open_file(@name, [])
      :ok = clear()
    end
  end

  @spec clear() :: :ok | :error
  def clear do
    :ok = Dets.delete_all_objects(@name)
    :ok = Dets.insert(@name, {:index, 1, 1})
  rescue
    _ -> :error
  end

  @spec save(data :: binary()) :: {:ok, binary()} | :error
  def save(data) do
    key = Crypto.hash(:md5, data)
    [{:index, last, next}] = Dets.lookup(@name, :index)
    :ok = Dets.insert(@name, {key, next, data})

    if next - last == 9 do
      maybe_delete_key(last)
      :ok = Dets.insert(@name, {:index, last + 1, next + 1})
    else
      :ok = Dets.insert(@name, {:index, last, next + 1})
    end

    {:ok, key}
  rescue
    _ -> :error
  end

  @spec lookup(key :: binary()) :: {:ok, binary()} | :not_found
  def lookup(key) do
    case Dets.lookup(@name, key) do
      [] -> :not_found
      [{_, _, data}] -> {:ok, data}
    end
  end

  @spec close() :: :ok | :error
  def close do
    :ok = Dets.close(@name)
  rescue
    _ -> :error
  end

  defp maybe_delete_key(last) do
    Dets.foldl(
      fn
        {:index, _, _}, acc ->
          acc

        {xkey, xlast, _}, false when xlast == last ->
          Dets.delete(@name, xkey)
          true

        _, acc ->
          acc
      end,
      false,
      @name
    )
  end
end
