defmodule Xylon.Datachannel.RateLimitTest do
  use ExUnit.Case, async: false

  alias Xylon.Datachannel.{Backend, RateLimit}
  alias Xylon.{Participant, Session}

  test "token bucket allows burst then denies" do
    plan = make_ref()
    RateLimit.ensure_table!()
    RateLimit.clear_plan(plan)

    cfg = {:token_bucket, 10, 3}

    assert :ok = RateLimit.take(plan, "a", cfg)
    assert :ok = RateLimit.take(plan, "a", cfg)
    assert :ok = RateLimit.take(plan, "a", cfg)
    assert :rate_limited = RateLimit.take(plan, "a", cfg)

    assert :ok = RateLimit.take(plan, "b", cfg)
  end

  test "nil config always allows" do
    assert :ok = RateLimit.take(make_ref(), "a", nil)
  end

  test "backend drops with :rate_limit telemetry when over limit" do
    parent = self()
    handler_id = "rate-limit-#{System.unique_integer([:positive])}"

    :ok =
      :telemetry.attach(
        handler_id,
        [:xylon, :dc, :drop],
        fn _e, _m, meta, _c -> send(parent, {:dc_drop, meta}) end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    session = %Session{id: "s1", tenant_id: "t1"}

    assert {:ok, state} =
             Backend.init(session,
               mode: :fanout_except_sender,
               rate_limit: {:token_bucket, 100, 1},
               labels: ["chat"]
             )

    a = %Participant{id: "a", peer_ref: self()}
    b = %Participant{id: "b", peer_ref: self()}
    assert {:ok, state} = Backend.participant_joined(a, state)
    assert {:ok, state} = Backend.participant_joined(b, state)

    ref_a = make_ref()
    ref_b = make_ref()
    assert {:ok, state} = Backend.handle_datachannel_open(a, %{ref: ref_a, label: "chat", id: 0}, state)
    assert {:ok, state} = Backend.handle_datachannel_open(b, %{ref: ref_b, label: "chat", id: 1}, state)

    assert {:forward_dc, [_], state} = Backend.handle_datachannel(a, ref_a, "one", state)
    assert {:ok, _} = Backend.handle_datachannel(a, ref_a, "two", state)
    assert_receive {:dc_drop, %{reason: :rate_limit}}, 200
  end

  test "clear_participant resets bucket" do
    plan = make_ref()
    RateLimit.ensure_table!()
    cfg = {:token_bucket, 1, 1}
    assert :ok = RateLimit.take(plan, "a", cfg)
    assert :rate_limited = RateLimit.take(plan, "a", cfg)
    assert :ok = RateLimit.clear_participant(plan, "a")
    assert :ok = RateLimit.take(plan, "a", cfg)
  end

  test "publish_datachannel is rate limited under shared :publish key" do
    parent = self()
    handler_id = "rate-limit-pub-#{System.unique_integer([:positive])}"

    :ok =
      :telemetry.attach(
        handler_id,
        [:xylon, :dc, :drop],
        fn _e, _m, meta, _c -> send(parent, {:dc_drop, meta}) end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    session = %Session{id: "s1", tenant_id: "t1"}

    assert {:ok, state} =
             Backend.init(session,
               mode: :fanout_except_sender,
               rate_limit: {:token_bucket, 100, 1},
               labels: ["chat"],
               default_label: "chat"
             )

    a = %Participant{id: "a", peer_ref: self()}
    assert {:ok, state} = Backend.participant_joined(a, state)
    ref_a = make_ref()
    assert {:ok, state} = Backend.handle_datachannel_open(a, %{ref: ref_a, label: "chat", id: 0}, state)

    assert {:forward_dc, [_], state} = Backend.publish_datachannel("one", :all, [], state)
    assert {:error, :rate_limited} = Backend.publish_datachannel("two", :all, [], state)
    assert_receive {:dc_drop, %{reason: :rate_limit}}, 200
  end
end
