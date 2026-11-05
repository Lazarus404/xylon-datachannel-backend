defmodule Xylon.Datachannel.Mfu do
  @moduledoc """
  Typed demux: decode -> type key -> handler / passthrough / drop.

  ## What problem this solves

  Opaque fanout treats every binary the same. Many apps put a logical message
  type inside JSON (or another codec) and want different handling: drop some
  types, rewrite others, or emit several outbound binaries. MFU (message fanout
  unit) runs only when Session mode is `:mfu`: decode with a `Codec`, look up
  the type, and apply handlers before Fanout.

  ## RFCs

  - Application framing only; not a wire RFC
  - Datachannel transport remains [RFC 8831](https://www.rfc-editor.org/rfc/rfc8831) / XGW
  """

  alias Xylon.Datachannel.Telemetry

  @typedoc """
  Result returned from an MFU type handler.

  ## Fields

  - `:drop` -- discard; no fanout
  - `{:forward, binary | term}` -- one outbound payload (term is encoded)
  - `{:emit, list}` -- many outbound payloads
  - optional third element -- dest override (`:all`, `:except_sender`, or id list)
  """
  @type handler_result ::
          :drop
          | {:forward, binary()}
          | {:emit, [binary()]}
          | {:forward, binary(), :all | :except_sender | [term()]}
          | {:emit, [binary()], :all | :except_sender | [term()]}

  @doc """
  Runs MFU on an inbound binary.

  Returns `{:ok, [{payload, dest_override | nil}]}`, `{:ok, :passthrough_unknown}`,
  or `:drop`. `dest_override` nil means use Session default dest policy.

  ## Examples

      iex> codec = Xylon.Datachannel.Codec.JSON
      iex> types = %{"chat" => :passthrough}
      iex> meta = %{session_id: "s", tenant_id: "t", mode: :mfu}
      iex> {:ok, [{bin, nil}]} =
      ...>   Xylon.Datachannel.Mfu.handle(~s({"type":"chat","text":"hi"}), codec, types, :drop, meta)
      iex> bin =~ "chat"
      true

      iex> codec = Xylon.Datachannel.Codec.JSON
      iex> meta = %{session_id: "s", tenant_id: "t", mode: :mfu}
      iex> Xylon.Datachannel.Mfu.handle(~s({"type":"x"}), codec, %{}, :drop, meta)
      :drop
  """
  @spec handle(binary(), module(), map(), atom(), map()) ::
          {:ok, [{binary(), nil | :all | :except_sender | [term()]}]}
          | {:ok, :passthrough_unknown}
          | :drop
  def handle(payload, codec, types, unknown_type, meta)
      when is_binary(payload) and is_atom(codec) and is_map(types) and is_map(meta) do
    case codec.decode(payload) do
      {:error, _} ->
        Telemetry.emit(:drop, %{count: 1}, Map.put(meta, :reason, :decode_error))
        :drop

      {:ok, decoded} ->
        type = codec.type_key(decoded)
        dispatch(type, decoded, payload, codec, types, unknown_type, meta)
    end
  end

  defp dispatch(nil, _decoded, _payload, _codec, _types, unknown_type, meta) do
    unknown(unknown_type, nil, meta)
  end

  defp dispatch(type, decoded, payload, codec, types, unknown_type, meta) do
    case Map.fetch(types, type) do
      :error ->
        # Also try string/atom synonym keys
        cond do
          is_binary(type) and Map.has_key?(types, String.to_existing_atom(type)) ->
            dispatch_handler(Map.fetch!(types, String.to_existing_atom(type)), decoded, payload, codec, meta)

          is_atom(type) and Map.has_key?(types, Atom.to_string(type)) ->
            dispatch_handler(Map.fetch!(types, Atom.to_string(type)), decoded, payload, codec, meta)

          true ->
            unknown(unknown_type, type, meta)
        end

      {:ok, :passthrough} ->
        {:ok, [{payload, nil}]}

      {:ok, fun} when is_function(fun, 3) ->
        dispatch_handler(fun, decoded, payload, codec, meta)

      {:ok, fun} when is_function(fun, 2) ->
        dispatch_handler(fn d, m -> fun.(d, m) end, decoded, payload, codec, meta)
    end
  rescue
    ArgumentError ->
      unknown(unknown_type, type, meta)
  end

  defp dispatch_handler(fun, decoded, _payload, codec, meta) when is_function(fun, 3) do
    apply_result(fun.(decoded, meta, codec), codec, meta)
  end

  defp dispatch_handler(fun, decoded, _payload, codec, meta) when is_function(fun, 2) do
    apply_result(fun.(decoded, meta), codec, meta)
  end

  defp apply_result(:drop, _codec, meta) do
    Telemetry.emit(:drop, %{count: 1}, Map.put(meta, :reason, :handler_drop))
    :drop
  end

  defp apply_result({:forward, msg}, codec, meta) when not is_binary(msg) do
    encode_one(msg, nil, codec, meta)
  end

  defp apply_result({:forward, bin}, _codec, _meta) when is_binary(bin) do
    {:ok, [{bin, nil}]}
  end

  defp apply_result({:forward, msg, dest}, codec, meta) when not is_binary(msg) do
    encode_one(msg, dest, codec, meta)
  end

  defp apply_result({:forward, bin, dest}, _codec, _meta) when is_binary(bin) do
    {:ok, [{bin, dest}]}
  end

  defp apply_result({:emit, list}, codec, meta) when is_list(list) do
    encode_many(list, nil, codec, meta)
  end

  defp apply_result({:emit, list, dest}, codec, meta) when is_list(list) do
    encode_many(list, dest, codec, meta)
  end

  defp apply_result(_other, _codec, meta) do
    Telemetry.emit(:drop, %{count: 1}, Map.put(meta, :reason, :handler_error))
    :drop
  end

  defp encode_one(msg, dest, codec, meta) do
    case codec.encode(msg) do
      {:ok, iodata} -> {:ok, [{IO.iodata_to_binary(iodata), dest}]}
      {:error, _} ->
        Telemetry.emit(:drop, %{count: 1}, Map.put(meta, :reason, :encode_error))
        :drop
    end
  end

  defp encode_many(list, dest, codec, meta) do
    Enum.reduce_while(list, [], fn item, acc ->
      bin =
        cond do
          is_binary(item) -> {:ok, item}
          true -> codec.encode(item)
        end

      case bin do
        {:ok, iodata} ->
          {:cont, [{IO.iodata_to_binary(iodata), dest} | acc]}

        {:error, _} ->
          Telemetry.emit(:drop, %{count: 1}, Map.put(meta, :reason, :encode_error))
          {:halt, :drop}
      end
    end)
    |> case do
      :drop -> :drop
      acc -> {:ok, Enum.reverse(acc)}
    end
  end

  defp unknown(:drop, _type, meta) do
    Telemetry.emit(:drop, %{count: 1}, Map.put(meta, :reason, :unknown_type))
    :drop
  end

  defp unknown(:passthrough, _type, _meta) do
    {:ok, :passthrough_unknown}
  end

  defp unknown(:error, _type, meta) do
    Telemetry.emit(:drop, %{count: 1}, Map.put(meta, :reason, :unknown_type_error))
    :drop
  end

  defp unknown(_other, type, meta), do: unknown(:drop, type, meta)
end
