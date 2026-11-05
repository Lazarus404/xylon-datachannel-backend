defmodule XylonDatachannelBackend.MixProject do
  use Mix.Project

  def project do
    [
      app: :xylon_datachannel_backend,
      version: "0.1.0",
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      description: "Xylon datachannel fanout and typed MFU backend.",
      source_url: "https://github.com/Lazarus404/xylon_datachannel_backend",
      docs: [
        extras: ["README.md"],
        main: "readme"
      ],
      package: package()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:xylon, "~> 0.1.0"},
      {:jason, "~> 1.4"},
      {:msgpax, "~> 2.4"},
      {:cbor, "~> 1.0"},
      {:telemetry, "~> 1.0"}
    ]
  end

  defp package do
    [
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => "https://github.com/Lazarus404/xylon_datachannel_backend"}
    ]
  end
end
