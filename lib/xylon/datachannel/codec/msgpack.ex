defmodule Xylon.Datachannel.Codec.Msgpack do
  @moduledoc """
  MessagePack codec for MFU. Logical type is the `"type"` field when present.

  ## What problem this solves

  High-rate `state` (and other typed) messages benefit from a compact binary
  framing while keeping the same map envelope shapes as JSON. This codec uses
  Msgpax for encode/decode and exposes `"type"` / `:type` as the MFU type key.

  ## RFCs

  - Application MessagePack framing only
  - Carried over [RFC 8831](https://www.rfc-editor.org/rfc/rfc8831) datachannels
  """

  @behaviour Xylon.Datachannel.Codec

  @doc """
  Decodes a MessagePack binary to a map (string keys preferred).

  ## Examples

      iex> {:ok, bin} = Xylon.Datachannel.Codec.Msgpack.encode(%{"type" => "chat"})
      iex> {:ok, map} = Xylon.Datachannel.Codec.Msgpack.decode(bin)
      iex> map["type"]
      "chat"
  """
  @impl true
  def decode(bin) when is_binary(bin) do
    case Msgpax.unpack(bin) do
      {:ok, decoded} -> {:ok, stringify_keys(decoded)}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Encodes a term to a MessagePack binary.

  ## Examples

      iex> {:ok, bin} = Xylon.Datachannel.Codec.Msgpack.encode(%{"type" => "ping"})
      iex> is_binary(bin)
      true
  """
  @impl true
  def encode(decoded) do
    case Msgpax.pack(decoded) do
      {:ok, iodata} -> {:ok, IO.iodata_to_binary(iodata)}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Returns the `"type"` / `:type` field, or nil.

  ## Examples

      iex> Xylon.Datachannel.Codec.Msgpack.type_key(%{"type" => "chat"})
      "chat"

      iex> Xylon.Datachannel.Codec.Msgpack.type_key(%{type: :ping})
      :ping

      iex> Xylon.Datachannel.Codec.Msgpack.type_key(%{"text" => "hi"})
      nil
  """
  @impl true
  def type_key(%{"type" => type}) when is_binary(type) or is_atom(type), do: type
  def type_key(%{type: type}) when is_binary(type) or is_atom(type), do: type
  def type_key(_), do: nil

  defp stringify_keys(map) when is_map(map) do
    Map.new(map, fn
      {k, v} when is_atom(k) -> {Atom.to_string(k), stringify_keys(v)}
      {k, v} when is_binary(k) -> {k, stringify_keys(v)}
      {k, v} -> {k, stringify_keys(v)}
    end)
  end

  defp stringify_keys(list) when is_list(list), do: Enum.map(list, &stringify_keys/1)
  defp stringify_keys(other), do: other
end
