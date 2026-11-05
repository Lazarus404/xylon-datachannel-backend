defmodule Xylon.Datachannel.TrunkDualNodeTest do
  use ExUnit.Case, async: false

  alias XGW.Trunk.Pipe
  alias Xylon.Control.{Dest, Groups, Membership}
  alias Xylon.Datachannel
  alias Xylon.Trunk.{Lab, Manager}
  alias Xylon.Trunk.Lab.SinkPeer
  alias Xylon.{Forwarder, Participant, Session}

  @moduletag :dual_node

  @key_ab <<0xE1F97A0D3E018BE0D64FA32C06DE4139::size(128)>>
  @salt_ab <<0x0EC675AD498AFEEBB6960B3AABE6::size(112)>>
  @key_ba <<0xA1B2C3D4E5F60718293A4B5C6D7E8F90::size(128)>>
  @salt_ba <<0x112233445566778899AABBCCDDEE::size(112)>>

  setup do
    Xylon.Catalogue.reset!()
    assert :ok = Datachannel.attach()
    :ok
  end

  test "opaque chat crosses dual-node trunk with label isolation" do
    ctx = setup_pair(mode_a: :fanout_except_sender)

    before = Pipe.stats(ctx.trunk_a.peer_ref)
    payload = "dual-node-chat"
    send(ctx.fwd_a, {:xgw, :datachannel_message, ctx.peer_a, ctx.ch_a, payload})

    assert wait_chat(ctx.peer_b, ctx.node_b, ctx.ch_b, payload)
    after_stats = Pipe.stats(ctx.trunk_a.peer_ref)
    assert after_stats.udp_out > before.udp_out
  end

  test "fanout_subset with trunk crosses dual-node; local outsider excluded" do
    ctx =
      setup_pair(
        mode_a: :fanout_subset,
        subset_a: ["trunk"],
        local_other?: true
      )

    before = Pipe.stats(ctx.trunk_a.peer_ref)
    payload = "subset-trunk-chat"
    send(ctx.fwd_a, {:xgw, :datachannel_message, ctx.peer_a, ctx.ch_a, payload})

    assert wait_chat(ctx.peer_b, ctx.node_b, ctx.ch_b, payload)
    after_stats = Pipe.stats(ctx.trunk_a.peer_ref)
    assert after_stats.udp_out > before.udp_out

    Process.sleep(40)
    assert SinkPeer.drain(ctx.peer_c) == []
  end

  test "client to trunk crosses; to local_only does not cascade" do
    ctx = setup_pair(mode_a: :fanout_except_sender, local_other?: true)

    cross = ~s({"type":"chat","text":"hop","to":["trunk"]})
    before = Pipe.stats(ctx.trunk_a.peer_ref)
    send(ctx.fwd_a, {:xgw, :datachannel_message, ctx.peer_a, ctx.ch_a, cross})
    assert wait_chat(ctx.peer_b, ctx.node_b, ctx.ch_b, cross)
    assert Pipe.stats(ctx.trunk_a.peer_ref).udp_out > before.udp_out

    _ = :erpc.call(ctx.node_b, SinkPeer, :drain, [ctx.peer_b])
    local = ~s({"type":"chat","text":"stay","to":["c"]})
    mid = Pipe.stats(ctx.trunk_a.peer_ref)
    send(ctx.fwd_a, {:xgw, :datachannel_message, ctx.peer_a, ctx.ch_a, local})

    assert wait_drain_local(ctx.peer_c, ctx.ch_c, local)
    Process.sleep(60)
    assert Pipe.stats(ctx.trunk_a.peer_ref).udp_out == mid.udp_out
    assert :erpc.call(ctx.node_b, SinkPeer, :drain, [ctx.peer_b]) == []
  end

  test "group-resolved publish including trunk crosses dual-node" do
    ctx = setup_pair(mode_a: :fanout_except_sender)
    tid = "t1"
    sid = ctx.sid

    :ok = Membership.join(tid, sid, "trunk", %{})
    :ok = Groups.create(tid, sid, "cascade")
    :ok = Groups.add_member(tid, sid, "cascade", "trunk")

    assert {:ok, ["trunk"]} = Dest.resolve(tid, sid, %{"group" => "cascade"})

    before = Pipe.stats(ctx.trunk_a.peer_ref)
    payload = "group-publish-hop"
    assert :ok = Datachannel.publish(ctx.session_a, payload, ["trunk"], label: "chat")

    assert wait_chat(ctx.peer_b, ctx.node_b, ctx.ch_b, payload)
    assert Pipe.stats(ctx.trunk_a.peer_ref).udp_out > before.udp_out
  end

  defp setup_pair(opts) do
    mode_a = Keyword.get(opts, :mode_a, :fanout_except_sender)
    subset_a = Keyword.get(opts, :subset_a, [])
    local_other? = Keyword.get(opts, :local_other?, false)

    assert {:ok, %{b: node_b, stop: stop}} = Lab.start_pair()
    on_exit(stop)

    assert :ok = :erpc.call(node_b, Xylon.Catalogue, :reset!, [])
    assert :ok = :erpc.call(node_b, Datachannel, :attach, [])

    sid = "dc-#{System.unique_integer([:positive])}"
    key = {"t1", sid}

    create_opts =
      case mode_a do
        :fanout_subset -> [mode: :fanout_subset, subset: subset_a]
        mode -> [mode: mode]
      end

    assert {:ok, session_a} = Session.create("t1", sid, :datachannel, create_opts)

    assert {:ok, trunk_a} =
             Manager.connect_remote(session_a, node_b,
               session_key: key,
               module_id: :datachannel,
               backend_opts: [mode: :fanout_except_sender],
               key_ab: @key_ab,
               salt_ab: @salt_ab,
               key_ba: @key_ba,
               salt_ba: @salt_ba
             )

    assert [{session_b, _}] = :erpc.call(node_b, Registry, :lookup, [Xylon.Registry, key])

    fwd_a = Session.forwarder(session_a)
    fwd_b = :erpc.call(node_b, Session, :forwarder, [session_b])
    pipe_b = :erpc.call(node_b, Manager, :trunk_pipe, [session_b, "trunk"])

    assert {:ok, peer_a} = SinkPeer.start_link()
    assert {:ok, peer_b} = :erpc.call(node_b, SinkPeer, :start, [])

    a = %Participant{id: "a", peer_ref: peer_a, role: :publisher}
    b = %Participant{id: "b", peer_ref: peer_b, role: :publisher}
    trunk_b = %Participant{id: "trunk", peer_ref: pipe_b, role: :trunk}

    assert :ok = Session.attach_participant(session_a, a)
    assert :ok = Forwarder.install_echo(fwd_a, a)
    assert :ok = Forwarder.install_echo(fwd_a, trunk_a)

    assert :ok = :erpc.call(node_b, Session, :attach_participant, [session_b, b])
    assert :ok = :erpc.call(node_b, Forwarder, :install_echo, [fwd_b, b])
    assert :ok = :erpc.call(node_b, Forwarder, :install_echo, [fwd_b, trunk_b])

    {peer_c, ch_c} =
      if local_other? do
        assert {:ok, peer_c} = SinkPeer.start_link()
        c = %Participant{id: "c", peer_ref: peer_c, role: :publisher}
        assert :ok = Session.attach_participant(session_a, c)
        assert :ok = Forwarder.install_echo(fwd_a, c)
        ch_c = make_ref()
        send(session_a, {:xgw, :datachannel, peer_c, %{ref: ch_c, label: "chat", id: 5}})
        {peer_c, ch_c}
      else
        {nil, nil}
      end

    ch_a = make_ref()
    ch_b = make_ref()
    assert {:ok, ch_ta} = Pipe.ensure_label(trunk_a.peer_ref, "chat")
    assert {:ok, ch_tb} = :erpc.call(node_b, Pipe, :ensure_label, [pipe_b, "chat"])

    send(session_a, {:xgw, :datachannel, peer_a, %{ref: ch_a, label: "chat", id: 1}})
    send(session_a, {:xgw, :datachannel, trunk_a.peer_ref, %{ref: ch_ta, label: "chat", id: 2}})

    _ =
      :erpc.call(node_b, Kernel, :send, [
        session_b,
        {:xgw, :datachannel, peer_b, %{ref: ch_b, label: "chat", id: 3}}
      ])

    _ =
      :erpc.call(node_b, Kernel, :send, [
        session_b,
        {:xgw, :datachannel, pipe_b, %{ref: ch_tb, label: "chat", id: 4}}
      ])

    Process.sleep(50)

    %{
      sid: sid,
      node_b: node_b,
      session_a: session_a,
      session_b: session_b,
      fwd_a: fwd_a,
      trunk_a: trunk_a,
      peer_a: peer_a,
      peer_b: peer_b,
      peer_c: peer_c,
      ch_a: ch_a,
      ch_b: ch_b,
      ch_c: ch_c
    }
  end

  defp wait_chat(peer_b, node_b, ch_b, payload) do
    Enum.reduce_while(1..80, false, fn _, _ ->
      events = :erpc.call(node_b, SinkPeer, :drain, [peer_b])

      if Enum.any?(events, &match?({:send_datachannel, ^ch_b, ^payload, _}, &1)) do
        {:halt, true}
      else
        Process.sleep(20)
        {:cont, false}
      end
    end)
    |> case do
      true -> true
      _ -> flunk("chat not delivered to edge B")
    end
  end

  defp wait_drain_local(peer, ch, payload) do
    Enum.reduce_while(1..40, false, fn _, _ ->
      events = SinkPeer.drain(peer)

      if Enum.any?(events, &match?({:send_datachannel, ^ch, ^payload, _}, &1)) do
        {:halt, true}
      else
        Process.sleep(20)
        {:cont, false}
      end
    end)
    |> case do
      true -> true
      _ -> flunk("local dest did not receive")
    end
  end
end
