defmodule Xylon.Datachannel.Codec.JSONTest do
  use ExUnit.Case, async: true

  alias Xylon.Datachannel.Codec.JSON

  test "round-trip and type_key" do
    assert {:ok, map} = JSON.decode(~s({"type":"chat","text":"hi"}))
    assert JSON.type_key(map) == "chat"
    assert {:ok, bin} = JSON.encode(map)
    assert {:ok, ^map} = JSON.decode(bin)
  end

  test "decode error" do
    assert {:error, _} = JSON.decode("not-json")
  end
end
