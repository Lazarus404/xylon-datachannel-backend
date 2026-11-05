defmodule Xylon.Datachannel.MfuTest do
  use ExUnit.Case, async: true

  alias Xylon.Datachannel.Codec.JSON
  alias Xylon.Datachannel.Mfu

  test "passthrough type forwards original payload" do
    payload = ~s({"type":"chat","text":"hi"})
    types = %{"chat" => :passthrough}

    assert {:ok, [{^payload, nil}]} = Mfu.handle(payload, JSON, types, :drop, %{})
  end

  test "unknown type drop" do
    payload = ~s({"type":"nope"})
    assert :drop = Mfu.handle(payload, JSON, %{}, :drop, %{})
  end

  test "handler can emit rewritten payload" do
    payload = ~s({"type":"move","x":1})

    types = %{
      "move" => fn decoded, _meta ->
        {:forward, Map.put(decoded, "ok", true)}
      end
    }

    assert {:ok, [{bin, nil}]} = Mfu.handle(payload, JSON, types, :drop, %{})
    assert {:ok, %{"type" => "move", "x" => 1, "ok" => true}} = JSON.decode(bin)
  end

  test "decode error drops" do
    assert :drop = Mfu.handle("{", JSON, %{}, :drop, %{})
  end
end
