# frozen_string_literal: true

require_relative "jobs"
require "sidekiq/web"
require "rack/session/cookie"
require "puma"
require "securerandom"

app = Rack::Builder.new do
  use Rack::Session::Cookie, secret: SecureRandom.hex(64), same_site: :lax
  run Sidekiq::Web
end.to_app

server = Puma::Server.new(app)
server.add_tcp_listener("127.0.0.1", 0)
thread = server.run
%w[TERM INT].each do |signal|
  Signal.trap(signal) { Thread.new { server.stop(true) } }
end
begin
  File.write(ENV.fetch("FFKV_SIDEKIQ_WEB_ADDRESS"), JSON.generate(
    "host" => "127.0.0.1", "port" => server.connected_ports.fetch(0)
  ))
  thread.join
ensure
  server.stop(true)
end
