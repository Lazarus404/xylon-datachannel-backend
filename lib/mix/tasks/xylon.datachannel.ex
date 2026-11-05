defmodule Mix.Tasks.Xylon.Datachannel do
  @moduledoc """
  Boot Xylon with the datachannel library as default, plus HTTP JSEP.

  ## What problem this solves

  Developers need a single Mix command to run a local Xylon server with this
  backend attached, a browser test client, and optional Control auth for inject
  and topics. This task configures backends, ICE advertisement, and Control
  mode, then starts the app and prints lab URLs.

  ## Usage

      XYLON_ADVERTISED_IP=127.0.0.1 mix xylon.datachannel --http-port 8080
      XYLON_ADVERTISED_IP=127.0.0.1 mix xylon.datachannel --http-port 8080 --control hmac
      XYLON_ADVERTISED_IP=127.0.0.1 mix xylon.datachannel --http-port 8080 --control jwt
      XYLON_ADVERTISED_IP=127.0.0.1 mix xylon.datachannel --http-port 8080 --control lab

  Open http://127.0.0.1:8080/ (or /client/) for the test client
  ([xylon-test-client](https://github.com/Lazarus404/xylon-test-client)).
  Create sessions with `library: "datachannel"` (or omit library; default is
  `:datachannel`). Requires `XTURN_LOCAL_DEPS=1` when using sibling checkouts.

  `--control hmac` enables `Xylon.Control` with TTL-HMAC bearer auth
  (`XYLON_CONTROL_HMAC_SECRET`, default `lab-hmac-secret`).
  Set `XYLON_CONTROL_SECRETS_URL` (e.g. `http://127.0.0.1:8091`) to use
  `Xylon.Control.Secrets.Local` against a secret store; otherwise Config adapter
  uses the global HMAC secret.
  `--control jwt` enables Control with JWKS JWT bearer (`XYLON_CONTROL_JWKS_URL`
  required; optional `XYLON_CONTROL_JWT_ISS` / `XYLON_CONTROL_JWT_AUD`).
  `--control lab` enables Control with `x-tenant-id` trust.
  Default is control off (header-only JSEP).

  Requires `XYLON_ADVERTISED_IP` (or `config :xylon, :ice`) for host candidates.
  Lab loopback: `XYLON_ADVERTISED_IP=127.0.0.1`. Optional Control WS full-ICE:
  `XYLON_ICE_MODE=full` and `XYLON_STUN_SERVERS=host:port,...`. See `Xylon.Ice`
  in [xylon](https://github.com/Lazarus404/xylon).
  Optional gRPC inject: `XYLON_CONTROL_GRPC_PORT=50051` (HMAC/JWT only).
  Optional webhook DLQ path: `XYLON_CONTROL_WEBHOOK_DLQ_PATH`.

  ## RFCs

  - [RFC 8829](https://www.rfc-editor.org/rfc/rfc8829) (JSEP over HTTP in the lab client)
  - [RFC 8831](https://www.rfc-editor.org/rfc/rfc8831) (datachannels under test)
  - [RFC 8445](https://www.rfc-editor.org/rfc/rfc8445) (ICE; configure via `Xylon.Ice`)
  """
  use Mix.Task

  require Logger

  @shortdoc "Run Xylon datachannel lab (HTTP JSEP)"

  @impl Mix.Task
  def run(args) do
    {opts, _} =
      OptionParser.parse!(args,
        strict: [http_port: :integer, control: :string],
        aliases: [h: :http_port]
      )

    port = Keyword.get(opts, :http_port, 8080)

    Application.put_env(:xylon, :http_port, port)

    Application.put_env(:xylon, :backends, [
      Xylon.Backend.Echo,
      Xylon.Datachannel.Backend
    ])

    Application.put_env(:xylon, :default_backend, :datachannel)

    require_ice!()
    apply_control(Keyword.get(opts, :control))

    Mix.Task.run("app.start")

    case System.get_env("XYLON_LOG_LEVEL") do
      level when level in ["emergency", "alert", "critical", "error", "warning", "warn", "notice", "info", "debug"] ->
        Logger.configure(level: String.to_existing_atom(level))

      _ ->
        :ok
    end

    Mix.shell().info("""
    xylon datachannel running
      HTTP   http://127.0.0.1:#{port}/
      Client http://127.0.0.1:#{port}/client/
      ICE    mode=#{Xylon.Ice.mode()} advertise #{Xylon.Ice.summary()}
      default backend #{inspect(Xylon.default_backend())}
      control #{control_summary()}
      log    #{inspect(Logger.level())}
    Lab tenant id: x-tenant-id header (HTTP JSEP) when control is off or auth:lab.
    Two-tab fanout: same session_id, different participant_id, label chat.
    Ctrl+C to stop.
    """)

    Process.sleep(:infinity)
  end

  defp require_ice! do
    Xylon.Ice.ensure_configured!()
  rescue
    e in ArgumentError -> Mix.raise(Exception.message(e))
  end

  defp apply_control(nil), do: :ok
  defp apply_control("off"), do: :ok

  defp apply_control("lab") do
    Application.put_env(:xylon, :control, base_control(auth: :lab))
  end

  defp apply_control("hmac") do
    secret = System.get_env("XYLON_CONTROL_HMAC_SECRET") || "lab-hmac-secret"

    hmac_opts =
      case System.get_env("XYLON_CONTROL_SECRETS_URL") do
        url when is_binary(url) and url != "" ->
          [auth: :hmac, hmac_secret: secret, secrets: {Xylon.Control.Secrets.Local, [url: url]}]

        _ ->
          [auth: :hmac, hmac_secret: secret]
      end

    Application.put_env(:xylon, :control, base_control(hmac_opts))
  end

  defp apply_control("jwt") do
    jwks_url = System.get_env("XYLON_CONTROL_JWKS_URL")

    unless is_binary(jwks_url) and jwks_url != "" do
      Mix.raise("--control jwt requires XYLON_CONTROL_JWKS_URL")
    end

    opts = [auth: :jwt, jwks_url: jwks_url]

    opts =
      case System.get_env("XYLON_CONTROL_JWT_ISS") do
        iss when is_binary(iss) and iss != "" -> Keyword.put(opts, :jwt_iss, iss)
        _ -> opts
      end

    opts =
      case System.get_env("XYLON_CONTROL_JWT_AUD") do
        aud when is_binary(aud) and aud != "" -> Keyword.put(opts, :jwt_aud, aud)
        _ -> opts
      end

    Application.put_env(:xylon, :control, base_control(opts))
  end

  defp apply_control(other) do
    Mix.raise("unknown --control #{inspect(other)} (use hmac, jwt, lab, or off)")
  end

  defp base_control(extra) do
    [
      enabled: true,
      topics: %{"chat" => %{label: "chat"}},
      quotas: %{max_participants: 50, inject_per_minute: 120, join_per_minute: 60},
      history_limit: 100,
      history_max_bytes: 4096
    ]
    |> Keyword.merge(extra)
    |> maybe_webhooks()
    |> maybe_grpc_port()
  end

  defp maybe_webhooks(opts) do
    opts
    |> put_env_opt(:webhook_url, "XYLON_CONTROL_WEBHOOK_URL")
    |> put_env_opt(:webhook_secret, "XYLON_CONTROL_WEBHOOK_SECRET")
    |> put_env_opt(:webhook_dlq_path, "XYLON_CONTROL_WEBHOOK_DLQ_PATH")
  end

  defp maybe_grpc_port(opts) do
    case System.get_env("XYLON_CONTROL_GRPC_PORT") do
      port_s when is_binary(port_s) and port_s != "" ->
        case Integer.parse(port_s) do
          {port, ""} when port > 0 -> Keyword.put(opts, :grpc_port, port)
          _ -> opts
        end

      _ ->
        opts
    end
  end

  defp put_env_opt(opts, key, env_name) do
    case Keyword.get(opts, key) || System.get_env(env_name) do
      val when is_binary(val) and val != "" -> Keyword.put(opts, key, val)
      _ -> opts
    end
  end

  defp control_summary do
    cfg = Application.get_env(:xylon, :control, [])

    if Keyword.get(cfg, :enabled, false) do
      auth = Keyword.get(cfg, :auth, :lab)
      parts = ["enabled auth=#{auth}"]

      parts =
        case Keyword.get(cfg, :grpc_port) do
          port when is_integer(port) and port > 0 -> parts ++ ["grpc_port=#{port}"]
          _ -> parts
        end

      parts =
        case Keyword.get(cfg, :webhook_url) do
          url when is_binary(url) and url != "" ->
            path = Keyword.get(cfg, :webhook_dlq_path) || "(tmp default)"
            parts ++ ["webhooks=on dlq=#{path}"]

          _ ->
            parts
        end

      Enum.join(parts, " ")
    else
      "disabled"
    end
  end
end
