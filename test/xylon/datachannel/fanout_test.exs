defmodule Xylon.Datachannel.FanoutTest do
  use ExUnit.Case, async: false

  alias Xylon.Datachannel.{Fanout, Plan}
  alias Xylon.Participant

  setup do
    plan = Plan.new()
    :ok = Plan.put_participant(plan, "a")
    :ok = Plan.put_participant(plan, "b")
    :ok = Plan.put_participant(plan, "c")

    ref_a = make_ref()
    ref_b = make_ref()
    ref_c = make_ref()
    :ok = Plan.put_channel(plan, "a", "chat", ref_a)
    :ok = Plan.put_channel(plan, "b", "chat", ref_b)
    :ok = Plan.put_channel(plan, "c", "chat", ref_c)

    participants = %{
      "a" => %Participant{id: "a", peer_ref: self()},
      "b" => %Participant{id: "b", peer_ref: self()},
      "c" => %Participant{id: "c", peer_ref: self()}
    }

    %{plan: plan, participants: participants, ref_a: ref_a, ref_b: ref_b, ref_c: ref_c}
  end

  test "all includes every open label", %{plan: plan, participants: participants, ref_a: a, ref_b: b, ref_c: c} do
    %{specs: specs, skipped: 0} = Fanout.resolve(plan, participants, :all, "a", "chat", "hi")
    refs = Enum.map(specs, fn {_p, ref, _} -> ref end) |> Enum.sort_by(&inspect/1)
    assert Enum.sort_by([a, b, c], &inspect/1) == refs
  end

  test "except_sender omits sender", %{plan: plan, participants: participants, ref_b: b, ref_c: c} do
    %{specs: specs, skipped: 0} =
      Fanout.resolve(plan, participants, :except_sender, "a", "chat", "hi")

    ids = Enum.map(specs, fn {p, _, _} -> p.id end) |> Enum.sort()
    assert ids == ["b", "c"]
    refs = Enum.map(specs, fn {_p, ref, _} -> ref end)
    assert b in refs and c in refs
  end

  test "subset uses explicit ids and skips missing channels", %{
    plan: plan,
    participants: participants,
    ref_b: b
  } do
    %{specs: specs, skipped: skipped} =
      Fanout.resolve(plan, participants, ["b", "missing"], nil, "chat", "hi")

    assert skipped == 1
    assert [{%{id: "b"}, ^b, "hi"}] = specs
  end

  test "skips participants without matching label", %{plan: plan, participants: participants} do
    %{specs: specs, skipped: skipped} =
      Fanout.resolve(plan, participants, :all, nil, "voice", "x")

    assert specs == []
    assert skipped == 3
  end
end
