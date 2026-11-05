defmodule Xylon.Datachannel.Envelope do
  @moduledoc """
  Shared JSON envelope shapes for datachannel labels (`chat`, `rpc`, `state`).

  ## What problem this solves

  SDK, MFU handlers, and lab clients need one contract for typed messages so
  RPC routing and state ticks do not invent ad hoc JSON per app. This module
  validates decoded maps and extracts common fields (`id`, `to`, `seq`, dest lists).

  ## RFCs

  - Application framing only; not a wire RFC
  - Carried over [RFC 8831](https://www.rfc-editor.org/rfc/rfc8831) datachannels
  - JSON text uses [RFC 8259](https://www.rfc-editor.org/rfc/rfc8259) via the MFU codec
  """

  @typedoc "Normalized envelope after validation."
  @type t :: map()

  @typedoc "Validated `rpc.req` destination: unicast id, room except sender, or id list."
  @type rpc_dest :: {:unicast, binary()} | :except_sender | {:ids, [binary()]}

  @doc """
  Validates a decoded JSON map against the Phase 1 wire contract.

  ## Examples

      iex> Xylon.Datachannel.Envelope.validate(%{
      ...>   "type" => "chat",
      ...>   "text" => "hi"
      ...> })
      {:ok, %{type: "chat", text: "hi"}}

      iex> Xylon.Datachannel.Envelope.validate(%{"type" => "rpc.req"})
      {:error, :invalid}
  """
  @spec validate(map()) :: {:ok, t()} | {:error, :invalid | :unknown_type}
  def validate(%{"type" => "rpc.req"} = m) do
    with id when is_binary(id) and id != "" <- Map.get(m, "id"),
         method when is_binary(method) and method != "" <- Map.get(m, "method"),
         {:ok, to} <- parse_rpc_to(Map.get(m, "to")),
         true <- Map.has_key?(m, "payload") do
      {:ok,
       %{
         type: "rpc.req",
         id: id,
         method: method,
         to: to,
         payload: Map.get(m, "payload"),
         from: optional_string(m, "from")
       }}
    else
      _ -> {:error, :invalid}
    end
  end

  def validate(%{"type" => "rpc.res"} = m) do
    with id when is_binary(id) and id != "" <- Map.get(m, "id"),
         true <- Map.has_key?(m, "payload") do
      {:ok,
       %{
         type: "rpc.res",
         id: id,
         payload: Map.get(m, "payload"),
         to: optional_string(m, "to")
       }}
    else
      _ -> {:error, :invalid}
    end
  end

  def validate(%{"type" => "rpc.err"} = m) do
    with id when is_binary(id) and id != "" <- Map.get(m, "id"),
         code when is_binary(code) and code != "" <- Map.get(m, "code"),
         message when is_binary(message) <- Map.get(m, "message") do
      {:ok,
       %{
         type: "rpc.err",
         id: id,
         code: code,
         message: message,
         to: optional_string(m, "to")
       }}
    else
      _ -> {:error, :invalid}
    end
  end

  def validate(%{"type" => "state"} = m) do
    with topic when is_binary(topic) and topic != "" <- Map.get(m, "topic"),
         seq when is_integer(seq) and seq >= 0 <- Map.get(m, "seq"),
         true <- Map.has_key?(m, "body") do
      {:ok,
       %{
         type: "state",
         topic: topic,
         seq: seq,
         body: Map.get(m, "body"),
         t: Map.get(m, "t")
       }}
    else
      _ -> {:error, :invalid}
    end
  end

  def validate(%{"type" => "chat"} = m) do
    case Map.get(m, "text") do
      text when is_binary(text) -> {:ok, %{type: "chat", text: text}}
      _ -> {:error, :invalid}
    end
  end

  def validate(%{"type" => _}), do: {:error, :unknown_type}
  def validate(_), do: {:error, :invalid}

  @doc """
  Returns the envelope id string, or nil.

  ## Examples

      iex> Xylon.Datachannel.Envelope.id(%{"id" => "abc"})
      "abc"
  """
  @spec id(map()) :: binary() | nil
  def id(%{"id" => id}) when is_binary(id), do: id
  def id(%{id: id}) when is_binary(id), do: id
  def id(_), do: nil

  @doc """
  Returns the destination participant id string, or nil.

  For client multicast targeting (string or id list), use `dest_ids/1`.

  ## Examples

      iex> Xylon.Datachannel.Envelope.to(%{"to" => "bob"})
      "bob"
  """
  @spec to(map()) :: binary() | nil
  def to(%{"to" => to}) when is_binary(to), do: to
  def to(%{to: to}) when is_binary(to), do: to
  def to(_), do: nil

  @doc """
  Parses client-originated `"to"` for media-path targeting.

  Returns `:omit` when absent, `{:ok, ids}` for a string or list of non-empty
  participant id strings (uniq, order preserved), or `:reject` for filters /
  empty / mixed types. Role or group maps must never reach Fanout.

  ## Examples

      iex> Xylon.Datachannel.Envelope.dest_ids(%{"text" => "hi"})
      :omit

      iex> Xylon.Datachannel.Envelope.dest_ids(%{"to" => "bob"})
      {:ok, ["bob"]}

      iex> Xylon.Datachannel.Envelope.dest_ids(%{"to" => ["a", "b", "a"]})
      {:ok, ["a", "b"]}

      iex> Xylon.Datachannel.Envelope.dest_ids(%{"to" => %{"role" => "mod"}})
      :reject
  """
  @spec dest_ids(map()) :: :omit | {:ok, [binary()]} | :reject
  def dest_ids(map) when is_map(map) do
    case Map.fetch(map, "to") do
      :error ->
        case Map.fetch(map, :to) do
          :error -> :omit
          {:ok, v} -> normalize_dest(v)
        end

      {:ok, v} ->
        normalize_dest(v)
    end
  end

  def dest_ids(_), do: :reject

  defp normalize_dest(nil), do: :omit

  defp normalize_dest(to) when is_binary(to) do
    if to != "", do: {:ok, [to]}, else: :reject
  end

  defp normalize_dest(list) when is_list(list) do
    if list != [] and Enum.all?(list, &(is_binary(&1) and &1 != "")) do
      {:ok, Enum.uniq(list)}
    else
      :reject
    end
  end

  defp normalize_dest(_), do: :reject

  @doc """
  Returns the state sequence number, or nil.

  ## Examples

      iex> Xylon.Datachannel.Envelope.seq(%{"seq" => 3})
      3
  """
  @spec seq(map()) :: non_neg_integer() | nil
  def seq(%{"seq" => seq}) when is_integer(seq) and seq >= 0, do: seq
  def seq(%{seq: seq}) when is_integer(seq) and seq >= 0, do: seq
  def seq(_), do: nil

  defp optional_string(map, key) do
    case Map.get(map, key) do
      v when is_binary(v) and v != "" -> v
      _ -> nil
    end
  end

  # RpcRouter-only: `"*"` means except_sender. Chat `dest_ids/1` does not use this.
  defp parse_rpc_to("*"), do: {:ok, :except_sender}

  defp parse_rpc_to(to) when is_binary(to) and to != "", do: {:ok, {:unicast, to}}

  defp parse_rpc_to(list) when is_list(list) do
    if list != [] and Enum.all?(list, &(is_binary(&1) and &1 != "")) do
      {:ok, {:ids, Enum.uniq(list)}}
    else
      :error
    end
  end

  defp parse_rpc_to(_), do: :error
end
