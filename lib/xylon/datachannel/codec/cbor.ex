defmodule Xylon.Datachannel.Codec.Cbor do
  @moduledoc """
  CBOR codec for MFU. Logical type is the `"type"` field when present.

  ## What problem this solves

  Embedded clients that speak CBOR need the same map envelope shapes as JSON /
  MessagePack. This codec uses the Hex `cbor` package and exposes `"type"` /
  `:type` as the MFU type key.

  ## RFCs

  - [RFC 8949](https://www.rfc-editor.org/rfc/rfc8949) -- Concise Binary Object Representation
  - Carried over [RFC 8831](https://www.rfc-editor.org/rfc/rfc8831) datachannels
  """

  @behaviour Xylon.Datachannel.Codec

  @doc """
  Decodes a CBOR binary to a map (string keys preferred).

  ## Examples

      iex> {:ok, bin} = Xylon.Datachannel.Codec.Cbor.encode(%{"type" => "chat"})
      iex> {:ok, map} = Xylon.Datachannel.Codec.Cbor.decode(bin)
      iex> map["type"]
      "chat"
  """
  @impl true
  def decode(bin) when is_binary(bin) do
    case CBOR.decode(bin) do
      {:ok, decoded, _rest} -> {:ok, stringify_keys(decoded)}
      {:error, reason} -> {:error, reason}
      other -> {:error, other}
    end
  end

  @doc """
  Encodes a term to a CBOR binary.

  ## Examples

      iex> {:ok, bin} = Xylon.Datachannel.Codec.Cbor.encode(%{"type" => "ping"})
      iex> is_binary(bin)
      true
  """
  @impl true
  def encode(decoded) do
    try do
      {:ok, CBOR.encode(decoded)}
    rescue
      e -> {:error, e}
    end
  end

  @doc """
  Returns the `"type"` / `:type` field, or nil.

  ## Examples

      iex> Xylon.Datachannel.Codec.Cbor.type_key(%{"type" => "chat"})
      "chat"
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
