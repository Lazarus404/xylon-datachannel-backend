defmodule Xylon.Datachannel.Codec do
  @moduledoc """
  Behaviour for MFU payload codecs.

  ## What problem this solves

  Typed demux (`Xylon.Datachannel.Mfu`) must turn an inbound binary into a
  structured value, pick a logical type key, and optionally encode handler
  results back to binaries. Apps plug in JSON, protobuf, or a custom format by
  implementing this behaviour. The default is `Xylon.Datachannel.Codec.JSON`.

  ## RFCs

  - Application codec only; not a wire RFC
  - Carried over [RFC 8831](https://www.rfc-editor.org/rfc/rfc8831) datachannels
  """

  @typedoc "Decoded application message (map, struct, or other term)."
  @type decoded :: term()

  @doc "Parses an inbound datachannel binary."
  @callback decode(binary()) :: {:ok, decoded()} | {:error, term()}

  @doc "Serializes a decoded value (or handler output) to iodata."
  @callback encode(decoded()) :: {:ok, iodata()} | {:error, term()}

  @doc "Logical type used to look up MFU handlers, or nil if absent."
  @callback type_key(decoded()) :: term() | nil
end
