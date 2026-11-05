defmodule Xylon.Datachannel.Fanout do
  @moduledoc """
  Resolves datachannel destinations and builds `{:forward_dc, ...}` specs.

  ## What problem this solves

  After deciding that a payload should leave the server, the Backend still needs
  a concrete list of `{participant, channel_ref, payload}` triples. Only peers
  that have the same label open can receive the message. This module applies a
  dest policy (`:all`, `:except_sender`, or an id list) against the Plan and
  skips missing channels.

  ## RFCs

  - [RFC 8831](https://www.rfc-editor.org/rfc/rfc8831) (label-scoped delivery)
  - Application routing policy only; wire send is Xylon/XGW
  """

  alias Xylon.Datachannel.Plan
  alias Xylon.Participant

  @typedoc "Who should receive a payload relative to the Plan membership."
  @type dest_policy :: :all | :except_sender | [term()]

  @typedoc """
  One outbound send instruction for the Session.

  ## Fields

  - participant -- `Xylon.Participant` with peer_ref for XGW send
  - channel_ref -- open datachannel ref on that peer for the label
  - payload -- binary to send
  """
  @type forward_spec :: {Participant.t(), reference(), binary()}

  @doc """
  Builds forward specs for `payload` on `label`.

  `participants` is `%{id => Participant.t()}` from the Session (or a snapshot).
  Missing matching open channels are skipped (count returned in `skipped`) and
  listed in `missing` as `{participant_id, label, payload}` for optional buffering.
  """
  @spec resolve(
          Plan.plan_ref(),
          %{optional(term()) => Participant.t()},
          dest_policy(),
          term() | nil,
          binary(),
          binary()
        ) :: %{
          specs: [forward_spec()],
          skipped: non_neg_integer(),
          missing: [{term(), binary(), binary()}]
        }
  def resolve(plan_ref, participants, dest, sender_id, label, payload)
      when is_binary(label) and is_binary(payload) do
    dest_ids = dest_ids(plan_ref, dest, sender_id)

    Enum.reduce(dest_ids, %{specs: [], skipped: 0, missing: []}, fn id, acc ->
      case Map.fetch(participants, id) do
        {:ok, participant} ->
          case Plan.channel_ref(plan_ref, id, label) do
            nil ->
              %{
                acc
                | skipped: acc.skipped + 1,
                  missing: [{id, label, payload} | acc.missing]
              }

            channel_ref ->
              %{acc | specs: [{participant, channel_ref, payload} | acc.specs]}
          end

        :error ->
          %{acc | skipped: acc.skipped + 1}
      end
    end)
    |> then(fn acc ->
      %{acc | specs: Enum.reverse(acc.specs), missing: Enum.reverse(acc.missing)}
    end)
  end

  defp dest_ids(plan_ref, :all, _sender_id) do
    Plan.participant_ids(plan_ref)
  end

  defp dest_ids(plan_ref, :except_sender, sender_id) do
    plan_ref
    |> Plan.participant_ids()
    |> Enum.reject(&(&1 == sender_id))
  end

  defp dest_ids(_plan_ref, ids, _sender_id) when is_list(ids), do: ids
end
