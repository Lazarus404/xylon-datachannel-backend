defmodule Xylon.Datachannel.StateCoalesceTest do
  use ExUnit.Case, async: false

  alias Xylon.Datachannel.{Backend, StateCoalesce}
  alias Xylon.{Participant, Session}

  test "stamp-only mfu adds from and fans out" do
    session = %Session{id: "s1", tenant_id: "t1"}

    assert {:ok, state} =
             Backend.init(session, mode: :mfu, labels: ["state"])

    a = %Participant{id: "a", peer_ref: self()}
    b = %Participant{id: "b", peer_ref: self()}
    assert {:ok, state} = Backend.participant_joined(a, state)
    assert {:ok, state} = Backend.participant_joined(b, state)

    ref_a = make_ref()
    ref_b = make_ref()
    assert {:ok, state} = Backend.handle_datachannel_open(a, %{ref: ref_a, label: "state", id: 0}, state)
    assert {:ok, state} = Backend.handle_datachannel_open(b, %{ref: ref_b, label: "state", id: 1}, state)

    payload =
      ~s({"type":"state","topic":"transform","seq":1,"body":{"x":1}})

    assert {:forward_dc, specs, _} = Backend.handle_datachannel(a, ref_a, payload, state)
    assert [{^b, ^ref_b, bin}] = specs
    assert {:ok, decoded} = Jason.decode(bin)
    assert decoded["from"] == "a"
    assert decoded["seq"] == 1
  end

  test "state_coalesce drops stale seq and forwards newer" do
    parent = self()
    handler_id = "state-coalesce-drop-#{System.unique_integer([:positive])}"

    :ok =
      :telemetry.attach(
        handler_id,
        [:xylon, :dc, :drop],
        fn _event, _meas, meta, _config -> send(parent, {:dc_drop, meta}) end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    session = %Session{id: "s1", tenant_id: "t1"}

    assert {:ok, state} =
             Backend.init(session,
               mode: :mfu,
               state_coalesce: true,
               labels: ["state"]
             )

    a = %Participant{id: "a", peer_ref: self()}
    b = %Participant{id: "b", peer_ref: self()}
    assert {:ok, state} = Backend.participant_joined(a, state)
    assert {:ok, state} = Backend.participant_joined(b, state)

    ref_a = make_ref()
    ref_b = make_ref()
    assert {:ok, state} = Backend.handle_datachannel_open(a, %{ref: ref_a, label: "state", id: 0}, state)
    assert {:ok, state} = Backend.handle_datachannel_open(b, %{ref: ref_b, label: "state", id: 1}, state)

    p1 = ~s({"type":"state","topic":"transform","seq":2,"body":{"x":2}})
    assert {:forward_dc, [_], state} = Backend.handle_datachannel(a, ref_a, p1, state)

    stale = ~s({"type":"state","topic":"transform","seq":1,"body":{"x":1}})
    assert {:ok, state} = Backend.handle_datachannel(a, ref_a, stale, state)
    assert_receive {:dc_drop, %{reason: :stale_state}}, 200

    same = ~s({"type":"state","topic":"transform","seq":2,"body":{"x":2}})
    assert {:ok, state} = Backend.handle_datachannel(a, ref_a, same, state)
    assert_receive {:dc_drop, %{reason: :stale_state}}, 200

    p3 = ~s({"type":"state","topic":"transform","seq":3,"body":{"x":3}})
    assert {:forward_dc, [{^b, ^ref_b, _}], state} =
             Backend.handle_datachannel(a, ref_a, p3, state)

    other = ~s({"type":"state","topic":"health","seq":1,"body":{"hp":10}})
    assert {:forward_dc, [{^b, ^ref_b, _}], _} =
             Backend.handle_datachannel(a, ref_a, other, state)
  end

  test "clear_participant removes seq rows" do
    plan = make_ref()
    StateCoalesce.ensure_table!()
    types = StateCoalesce.types_map(plan, [])
    meta = %{sender_id: "a", label: "state", session_id: "s", tenant_id: "t", mode: :mfu}

    assert {:forward, _} =
             types["state"].(
               %{"type" => "state", "topic" => "t", "seq" => 5, "body" => %{}},
               meta
             )

    assert :ok = StateCoalesce.clear_participant(plan, "a")

    assert {:forward, _} =
             types["state"].(
               %{"type" => "state", "topic" => "t", "seq" => 1, "body" => %{}},
               meta
             )
  end
end
