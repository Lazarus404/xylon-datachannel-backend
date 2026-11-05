defmodule Xylon.Datachannel.EgressRouteTest do
  use ExUnit.Case, async: false

  alias Xylon.Datachannel.EgressRoute

  setup do
    plan = make_ref()
    EgressRoute.clear_plan(plan)
    {:ok, plan: plan}
  end

  test "put get clear with label fallback", %{plan: plan} do
    assert :ok = EgressRoute.put(plan, "a", ["b", "c"])
    assert {:ok, ["b", "c"]} = EgressRoute.get(plan, "a", "chat")
    assert {:ok, ["b", "c"]} = EgressRoute.get(plan, "a", :_)

    assert :ok = EgressRoute.put(plan, "a", ["d"], "chat")
    assert {:ok, ["d"]} = EgressRoute.get(plan, "a", "chat")
    assert {:ok, ["b", "c"]} = EgressRoute.get(plan, "a", "state")

    assert :ok = EgressRoute.clear(plan, "a", "chat")
    assert {:ok, ["b", "c"]} = EgressRoute.get(plan, "a", "chat")

    assert :ok = EgressRoute.clear(plan, "a")
    assert :default = EgressRoute.get(plan, "a", "chat")
  end

  test "clear_participant removes sender rows and prunes dest lists", %{plan: plan} do
    assert :ok = EgressRoute.put(plan, "a", ["b", "c"])
    assert :ok = EgressRoute.put(plan, "a", ["b"], "chat")
    assert :ok = EgressRoute.put(plan, "c", ["a", "b"])
    assert :ok = EgressRoute.clear_participant(plan, "a")
    assert :default = EgressRoute.get(plan, "a", "chat")
    assert {:ok, ["b"]} = EgressRoute.get(plan, "c", :_)
  end

  test "clear_sender keeps other routes intact", %{plan: plan} do
    assert :ok = EgressRoute.put(plan, "a", ["b"], "chat")
    assert :ok = EgressRoute.put(plan, "a", :all)
    assert :ok = EgressRoute.put(plan, "b", ["a"])
    assert :ok = EgressRoute.clear_sender(plan, "a")
    assert :default = EgressRoute.get(plan, "a", "chat")
    assert {:ok, ["a"]} = EgressRoute.get(plan, "b", :_)
  end

  test "rejects bad dest", %{plan: plan} do
    assert {:error, :bad_dest} = EgressRoute.put(plan, "a", %{role: "mod"})
  end
end
