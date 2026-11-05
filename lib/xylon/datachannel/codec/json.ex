defmodule Xylon.Datachannel.Codec.JSON do
  @moduledoc """
  JSON codec for MFU. Logical type is the `"type"` field when present.

  ## What problem this solves

  Many chat and control messages are JSON objects with a `"type"` (or `:type`)
  field. This codec uses Jason for decode/encode and exposes that field as the
  MFU type key so Session `types` maps can dispatch handlers without a custom
  codec.

  ## RFCs

  - Application JSON framing only
  - [RFC 8259](https://www.rfc-editor.org/rfc/rfc8259) (JSON) when messages are JSON text
  - Carried over [RFC 8831](https://www.rfc-editor.org/rfc/rfc8831) datachannels
  """

  @behaviour Xylon.Datachannel.Codec

  @doc """
  Decodes a JSON binary.

  ## Examples

      iex> Xylon.Datachannel.Codec.JSON.decode(~s({"type":"chat","text":"hi"}))
      {:ok, %{"type" => "chat", "text" => "hi"}}

      iex> match?({:error, %Jason.DecodeError{}}, Xylon.Datachannel.Codec.JSON.decode("not-json"))
      true
  """
  @impl true
  def decode(bin) when is_binary(bin) do
    case Jason.decode(bin) do
      {:ok, decoded} -> {:ok, decoded}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Encodes a term to a JSON binary.

  ## Examples

      iex> {:ok, bin} = Xylon.Datachannel.Codec.JSON.encode(%{"type" => "ping"})
      iex> bin
      ~s({"type":"ping"})
  """
  @impl true
  def encode(decoded) do
    case Jason.encode(decoded) do
      {:ok, bin} -> {:ok, bin}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Returns the `"type"` / `:type` field, or nil.

  ## Examples

      iex> Xylon.Datachannel.Codec.JSON.type_key(%{"type" => "chat"})
      "chat"

      iex> Xylon.Datachannel.Codec.JSON.type_key(%{type: :ping})
      :ping

      iex> Xylon.Datachannel.Codec.JSON.type_key(%{"text" => "hi"})
      nil
  """
  @impl true
  def type_key(%{"type" => type}) when is_binary(type) or is_atom(type), do: type
  def type_key(%{type: type}) when is_binary(type) or is_atom(type), do: type
  def type_key(_), do: nil
end
