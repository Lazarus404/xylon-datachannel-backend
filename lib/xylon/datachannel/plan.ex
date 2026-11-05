defmodule Xylon.Datachannel.Plan do
  @moduledoc """
  ETS membership and label -> channel_ref maps for a datachannel Session.

  ## What problem this solves

  Fanout needs a fast answer to "who is in this room?" and "does participant P
  have label L open?". That graph is shared between Session-owned Backend state
  and any Forwarder path that also calls `Backend.init/2`. This module stores
  membership and per-participant channel maps in a named ETS table keyed by
  `plan_ref` (normally the Session pid).

  Membership uses one ETS row per participant (`{plan_ref, :member, id}`) so
  concurrent `put_participant` from Session and Forwarder cannot lose ids via
  read-modify-write on a shared MapSet. Call `Plan.init/0` (or `attach/0`) from
  a long-lived boot process so the ETS table is not owned by a Session that can
  crash and wipe every room's membership.

  ## RFCs

  - [RFC 8831](https://www.rfc-editor.org/rfc/rfc8831) (datachannel labels as isolation keys)
  - No SDP/JSEP parsing here; channel open/close events come from XGW via Xylon
  """

  alias Xylon.Participant

  @table :xylon_datachannel_plan

  @typedoc "Opaque plan key: Session pid or a fresh `reference()`."
  @type plan_ref :: pid() | reference()

  @typedoc """
  Open channels for one participant.

  ## Fields

  - keys -- datachannel label binaries
  - values -- XGW channel refs used for send
  """
  @type channel_map :: %{optional(binary()) => reference()}

  @doc "Creates the Plan ETS table if missing (call from Application boot)."
  @spec init() :: :ok
  def init, do: ensure_table!()

  @doc """
  Ensures the named ETS table exists.

  The first caller owns the table. Prefer `init/0` from a long-lived process
  (`Xylon.Application` or `Xylon.Datachannel.attach/0` at boot) so a Session
  crash cannot delete Plan rows for every live session.
  """
  @spec ensure_table!() :: :ok
  def ensure_table! do
    case :ets.whereis(@table) do
      :undefined ->
        _ =
          :ets.new(@table, [
            :named_table,
            :public,
            :set,
            read_concurrency: true,
            write_concurrency: true
          ])

        :ok

      _tid ->
        :ok
    end
  end

  @doc """
  Creates or reuses a plan under `plan_ref`.

  When `plan_ref` is omitted, allocates `make_ref()`. Passing the Session pid
  lets a second `Backend.init/2` (Forwarder) share the same ETS graph.
  """
  @spec new() :: plan_ref()
  @spec new(plan_ref()) :: plan_ref()
  def new(plan_ref \\ nil) do
    ensure_table!()
    ref = plan_ref || make_ref()

    case :ets.lookup(@table, {ref, :meta}) do
      [] ->
        true = :ets.insert(@table, {{ref, :meta}, %{}})

      _ ->
        :ok
    end

    ref
  end

  @doc "Drops all rows for `plan_ref`."
  @spec delete(plan_ref()) :: :ok
  def delete(plan_ref) do
    ensure_table!()

    :ets.match_delete(@table, {{plan_ref, :_}, :_})
    :ets.match_delete(@table, {{plan_ref, :_, :_}, :_})
    :ok
  end

  @doc "Registers a participant id (and optional `Participant` struct) in the plan."
  @spec put_participant(plan_ref(), term() | Participant.t()) :: :ok
  def put_participant(plan_ref, %Participant{id: participant_id} = participant) do
    ensure_table!()
    put_participant_id(plan_ref, participant_id)
    true = :ets.insert(@table, {{plan_ref, :participant, participant_id}, participant})
    :ok
  end

  def put_participant(plan_ref, participant_id) do
    ensure_table!()
    put_participant_id(plan_ref, participant_id)
  end

  @doc "Removes a participant, stored struct, and their channel map."
  @spec delete_participant(plan_ref(), term()) :: :ok
  def delete_participant(plan_ref, participant_id) do
    ensure_table!()
    true = :ets.delete(@table, {plan_ref, :member, participant_id})
    true = :ets.delete(@table, {plan_ref, :channels, participant_id})
    true = :ets.delete(@table, {plan_ref, :participant, participant_id})
    :ok
  end

  @doc "Looks up a stored `Participant` struct, or nil."
  @spec get_participant(plan_ref(), term()) :: Participant.t() | nil
  def get_participant(plan_ref, participant_id) do
    ensure_table!()

    case :ets.lookup(@table, {plan_ref, :participant, participant_id}) do
      [{_, %Participant{} = p}] -> p
      _ -> nil
    end
  end

  @doc "Builds `%{id => Participant}` from stored structs (missing structs omitted)."
  @spec participants_map(plan_ref()) :: %{optional(term()) => Participant.t()}
  def participants_map(plan_ref) do
    plan_ref
    |> participant_ids()
    |> Enum.reduce(%{}, fn id, acc ->
      case get_participant(plan_ref, id) do
        %Participant{} = p -> Map.put(acc, id, p)
        _ -> acc
      end
    end)
  end

  @doc "Records an open datachannel for `participant_id` under `label`."
  @spec put_channel(plan_ref(), term(), binary(), reference()) :: :ok
  def put_channel(plan_ref, participant_id, label, channel_ref)
      when is_binary(label) and is_reference(channel_ref) do
    ensure_table!()
    # Membership first: reopen after SCTP bounce must restore the participant id.
    put_participant_id(plan_ref, participant_id)
    map = get_channels(plan_ref, participant_id)
    true = :ets.insert(@table, {{plan_ref, :channels, participant_id}, Map.put(map, label, channel_ref)})
    :ok
  end

  @doc "Removes one channel_ref (any label) for the participant."
  @spec delete_channel(plan_ref(), term(), reference()) :: :ok
  def delete_channel(plan_ref, participant_id, channel_ref) do
    ensure_table!()
    map = get_channels(plan_ref, participant_id)

    map =
      Enum.reduce(map, %{}, fn {label, ref}, acc ->
        if ref == channel_ref, do: acc, else: Map.put(acc, label, ref)
      end)

    true = :ets.insert(@table, {{plan_ref, :channels, participant_id}, map})
    :ok
  end

  @doc "Clears all open channels for a participant (membership kept)."
  @spec clear_channels(plan_ref(), term()) :: :ok
  def clear_channels(plan_ref, participant_id) do
    ensure_table!()
    true = :ets.insert(@table, {{plan_ref, :channels, participant_id}, %{}})
    :ok
  end

  @doc "All participant ids in the plan."
  @spec participant_ids(plan_ref()) :: [term()]
  def participant_ids(plan_ref) do
    ensure_table!()

    :ets.select(@table, [
      {{{plan_ref, :member, :"$1"}, :_}, [], [:"$1"]}
    ])
  end

  @doc "Label -> channel_ref map for one participant."
  @spec get_channels(plan_ref(), term()) :: channel_map()
  def get_channels(plan_ref, participant_id) do
    ensure_table!()

    case :ets.lookup(@table, {plan_ref, :channels, participant_id}) do
      [{_, map}] when is_map(map) -> map
      _ -> %{}
    end
  end

  @doc "Looks up channel_ref for `label`, or nil."
  @spec channel_ref(plan_ref(), term(), binary()) :: reference() | nil
  def channel_ref(plan_ref, participant_id, label) when is_binary(label) do
    Map.get(get_channels(plan_ref, participant_id), label)
  end

  @doc "Finds label for a channel_ref on this participant, or nil."
  @spec label_for_ref(plan_ref(), term(), reference()) :: binary() | nil
  def label_for_ref(plan_ref, participant_id, channel_ref) do
    plan_ref
    |> get_channels(participant_id)
    |> Enum.find_value(fn {label, ref} -> if ref == channel_ref, do: label end)
  end

  defp put_participant_id(plan_ref, participant_id) do
    # One row per member: concurrent puts cannot clobber each other.
    true = :ets.insert(@table, {{plan_ref, :member, participant_id}, true})

    case :ets.lookup(@table, {plan_ref, :channels, participant_id}) do
      [] -> true = :ets.insert(@table, {{plan_ref, :channels, participant_id}, %{}})
      _ -> :ok
    end

    :ok
  end
end
