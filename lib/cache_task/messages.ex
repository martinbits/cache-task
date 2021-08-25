defmodule CacheTask.Messages do
  @moduledoc """
  The on the wire XDR dataplance protocol.
  """

  @cache_raw_message 0
  @cache_block_message 1
  @cache_reference_message 2
  @cache_reference_ok_message 3
  @cache_missed_reference_message 4
  @cache_missed_reference_info_message 5

  defmacro cache_raw_message, do: @cache_raw_message
  defmacro cache_block_message, do: @cache_block_message
  defmacro cache_reference_message, do: @cache_reference_message
  defmacro cache_reference_ok_message, do: @cache_reference_ok_message
  defmacro cache_missed_reference_message, do: @cache_missed_reference_message
  defmacro cache_missed_reference_info_message, do: @cache_missed_reference_info_message

  @spec raw_message(data :: binary) :: binary

  @doc """
  Generates a raw message containing the specified data.

  ## Examples

      iex(1)> CacheTask.Messages.raw_message(<<1, 2, 3>>)
      <<0, 0, 3, 1, 2, 3>>

  """
  def raw_message(data) when is_binary(data),
    do: <<@cache_raw_message::8, byte_size(data)::16, data::binary>>

  @spec block_message(data :: binary) :: binary

  @doc """
  Generates a raw message containing the specified data with the given block id.

  ## Examples

      iex(1)> CacheTask.Messages.block_message(<<1, 2, 3>>)
      <<1, 0, 3, 1, 2, 3>>

  """
  def block_message(data) when is_binary(data),
    do: <<@cache_block_message::8, byte_size(data)::16, data::binary>>

  @spec reference_message(key :: binary) :: binary

  @doc """
  Generates a reference message using the given id.

  ## Examples

      iex(1)> CacheTask.Messages.reference_message(:crypto.hash(:md5, <<1, 2, 3>>))
      <<2, 82, 137, 223, 115, 125, 245, 115, 38, 252, 221, 34, 89, 122, 251, 31, 172>>

  """
  def reference_message(key) when byte_size(key) == 16,
    do: <<@cache_reference_message::8, key::binary>>

  @spec reference_ok_message(key :: binary) :: binary

  @doc """
  Generates a reference ok message using the given id.

  ## Examples

      iex(1)> CacheTask.Messages.reference_ok_message(:crypto.hash(:md5, <<1, 2, 3>>))
      <<3, 82, 137, 223, 115, 125, 245, 115, 38, 252, 221, 34, 89, 122, 251, 31, 172>>

  """
  def reference_ok_message(key) when byte_size(key) == 16,
    do: <<@cache_reference_ok_message::8, key::binary>>

  @spec missed_reference_message(key :: binary) :: binary

  @doc """
  Generates a missed reference message using the given id.

  ## Examples

      iex(1)> CacheTask.Messages.missed_reference_message(:crypto.hash(:md5, <<1, 2, 3>>))
      <<4, 82, 137, 223, 115, 125, 245, 115, 38, 252, 221, 34, 89, 122, 251, 31, 172>>

  """
  def missed_reference_message(key) when byte_size(key) == 16,
    do: <<@cache_missed_reference_message::8, key::binary>>

  @spec missed_reference_info_message(data :: binary) :: binary

  @doc """
  Sent in response to a cache miss message. Provides the missing block.

  ## Examples

      iex(1)> CacheTask.Messages.missed_reference_info_message(<<1, 2, 3>>)
      <<5, 0, 3, 1, 2, 3>>

  """
  def missed_reference_info_message(data),
    do: <<@cache_missed_reference_info_message::8, byte_size(data)::16, data::binary>>
end
