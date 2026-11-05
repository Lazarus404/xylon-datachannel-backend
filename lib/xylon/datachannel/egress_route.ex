defmodule Xylon.Datachannel.EgressRoute do
  @moduledoc """
  Per-sender default destination map for inbound datachannel fanout.

  ## What problem this solves

  Hosts need dynamic visibility / interest as participant id sets without
  renegotiating Session attach opts. This module stores
  `{plan_ref, sender_id, label | :_}` -> `:all` | `:except_sender` | `[ids]`
  in ETS. Backend consults it after MFU `dest_override` and client `"to"`,
  before Session `mode_dest`. Apps compute AOI outside Xylon; this store holds
  routes only.

  ## Internal

  Used from `Xylon.Datachannel.Backend` and Control `set_route` / `clear_route`.

  ## RFCs

  - Application policy only; no wire RFC
  """

  @table :xylon_datachannel_egress

  @type dest :: :all | :except_sender | [term()]
  @type label_key :: binary() | :_

  @doc "Ensures the egress ETS table exists."
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
  Stores a route for `sender_id`.

  `label` is a datachannel label string or `:_` (label-agnostic default).
  `dest` must be `:all`, `:except_sender`, or a (possibly empty) id list.
  """
  @spec put(term(), term(), dest(), label_key()) :: :ok | {:error, :bad_dest}
  def put(plan_ref, sender_id, dest, label \\ :_) do
    with {:ok, dest} <- normalize_dest(dest),
         {:ok, label} <- normalize_label(label) do
      ensure_table!()
      true = :ets.insert(@table, {{plan_ref, sender_id, label}, dest})
      :ok
    end
  end

  @doc "Removes one route row (`label` defaults to `:_`)."
  @spec clear(term(), term(), label_key()) :: :ok
  def clear(plan_ref, sender_id, label \\ :_) do
    ensure_table!()

    case normalize_label(label) do
      {:ok, lab} ->
        _ = :ets.delete(@table, {plan_ref, sender_id, lab})
        :ok

      {:error, _} ->
        :ok
    end
  end

  @doc """
  Looks up a route: label-specific first, then `:_`.

  Returns `{:ok, dest}` or `:default` when no row applies.
  """
  @spec get(term(), term(), term()) :: {:ok, dest()} | :default
  def get(plan_ref, sender_id, label) do
    ensure_table!()
    lab = if is_binary(label) and label != "", do: label, else: :_

    case :ets.lookup(@table, {plan_ref, sender_id, lab}) do
      [{_, dest}] ->
        {:ok, dest}

      [] when lab != :_ ->
        case :ets.lookup(@table, {plan_ref, sender_id, :_}) do
          [{_, dest}] -> {:ok, dest}
          [] -> :default
        end

      [] ->
        :default
    end
  end

  @doc "All routes for a plan as maps `%{sender_id, label, dest}` (label `nil` for `:_`)."
  @spec list(term()) :: [map()]
  def list(plan_ref) do
    ensure_table!()

    @table
    |> :ets.match({{plan_ref, :"$1", :"$2"}, :"$3"})
    |> Enum.map(fn [sender_id, label, dest] ->
      %{
        sender_id: sender_id,
        label: if(label == :_, do: nil, else: label),
        dest: encode_dest(dest)
      }
    end)
  end

  @doc "Drops every route for one sender (on leave)."
  @spec clear_participant(term(), term()) :: :ok
  def clear_participant(plan_ref, participant_id) do
    ensure_table!()
    clear_sender(plan_ref, participant_id)
    prune_dest(plan_ref, participant_id)
    :ok
  end

  @doc "Deletes all label rows for `sender_id` (does not prune them from other routes)."
  @spec clear_sender(term(), term()) :: :ok
  def clear_sender(plan_ref, sender_id) do
    ensure_table!()
    _ = :ets.match_delete(@table, {{plan_ref, sender_id, :_}, :_})
    :ok
  end

  @doc """
  Removes `participant_id` from every id-list dest in the plan.

  `:all` / `:except_sender` rows are unchanged. Empty lists remain (empty-route drop).
  """
  @spec prune_dest(term(), term()) :: :ok
  def prune_dest(plan_ref, participant_id) do
    ensure_table!()

    # ponytail: O(routes) scan per leave; fine for room-sized maps. Upgrade: reverse index.
    @table
    |> :ets.match_object({{plan_ref, :_, :_}, :_})
    |> Enum.each(fn {{^plan_ref, sender, label}, dest} ->
      case dest do
        list when is_list(list) ->
          pruned = Enum.reject(list, &(&1 == participant_id))

          if pruned != list do
            true = :ets.insert(@table, {{plan_ref, sender, label}, pruned})
          end

        _ ->
          :ok
      end
    end)

    :ok
  end

  @doc "Drops all routes for a plan (Session terminate)."
  @spec clear_plan(term()) :: :ok
  def clear_plan(plan_ref) do
    ensure_table!()
    _ = :ets.match_delete(@table, {{plan_ref, :_, :_}, :_})
    :ok
  end

  defp normalize_dest(:all), do: {:ok, :all}
  defp normalize_dest(:except_sender), do: {:ok, :except_sender}
  defp normalize_dest(ids) when is_list(ids), do: {:ok, Enum.uniq(ids)}
  defp normalize_dest(_), do: {:error, :bad_dest}

  defp normalize_label(:_), do: {:ok, :_}
  defp normalize_label(label) when is_binary(label) and label != "", do: {:ok, label}
  defp normalize_label(nil), do: {:ok, :_}
  defp normalize_label(_), do: {:error, :bad_dest}

  defp encode_dest(:all), do: "all"
  defp encode_dest(:except_sender), do: "except"
  defp encode_dest(ids) when is_list(ids), do: ids
end
