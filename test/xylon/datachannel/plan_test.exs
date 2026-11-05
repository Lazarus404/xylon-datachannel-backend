defmodule Xylon.Datachannel.PlanTest do
  use ExUnit.Case, async: false

  alias Xylon.Datachannel.Plan

  test "tracks participants and label channels" do
    plan = Plan.new()
    assert :ok = Plan.put_participant(plan, "a")
    assert :ok = Plan.put_participant(plan, "b")
    assert Enum.sort(Plan.participant_ids(plan)) == ["a", "b"]

    ref_a = make_ref()
    ref_b = make_ref()
    assert :ok = Plan.put_channel(plan, "a", "chat", ref_a)
    assert :ok = Plan.put_channel(plan, "b", "chat", ref_b)

    assert Plan.channel_ref(plan, "a", "chat") == ref_a
    assert Plan.label_for_ref(plan, "a", ref_a) == "chat"
    assert Plan.channel_ref(plan, "a", "other") == nil

    assert :ok = Plan.delete_channel(plan, "a", ref_a)
    assert Plan.channel_ref(plan, "a", "chat") == nil

    assert :ok = Plan.put_channel(plan, "b", "chat", ref_b)
    assert :ok = Plan.clear_channels(plan, "b")
    assert Plan.channel_ref(plan, "b", "chat") == nil
    assert Plan.participant_ids(plan) == ["a", "b"]

    assert :ok = Plan.delete_participant(plan, "a")
    assert Plan.participant_ids(plan) == ["b"]

    assert :ok = Plan.delete(plan)
  end

  test "new/1 reuses meta for the same plan_ref" do
    ref = make_ref()
    assert ^ref = Plan.new(ref)
    assert :ok = Plan.put_participant(ref, "a")
    assert ^ref = Plan.new(ref)
    assert Plan.participant_ids(ref) == ["a"]
  end

  test "put_participant/2 stores Participant for participants_map" do
    plan = Plan.new()
    a = %Xylon.Participant{id: "a", peer_ref: self()}
    assert :ok = Plan.put_participant(plan, a)
    assert %{"a" => ^a} = Plan.participants_map(plan)
  end

  test "put_channel/4 restores membership when participant was removed" do
    plan = Plan.new()
    assert :ok = Plan.put_participant(plan, "a")
    assert :ok = Plan.put_participant(plan, "b")
    assert :ok = Plan.delete_participant(plan, "a")
    assert Plan.participant_ids(plan) == ["b"]

    ref = make_ref()
    assert :ok = Plan.put_channel(plan, "a", "chat", ref)
    assert Enum.sort(Plan.participant_ids(plan)) == ["a", "b"]
    assert Plan.label_for_ref(plan, "a", ref) == "chat"
  end

  test "concurrent put_participant does not lose members" do
    plan = Plan.new()
    ids = Enum.map(1..40, &"p#{&1}")

    ids
    |> Task.async_stream(
      fn id -> Plan.put_participant(plan, id) end,
      max_concurrency: 20,
      timeout: 5_000
    )
    |> Stream.run()

    assert Enum.sort(Plan.participant_ids(plan)) == Enum.sort(ids)
  end
end
