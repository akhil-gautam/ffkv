# frozen_string_literal: true

require "sidekiq"
require "json"
require "logger"

redis_options = {url: ENV.fetch("FFKV_SIDEKIQ_URL"), network_timeout: 2, pool_timeout: 3}
Sidekiq.configure_client do |config|
  config.redis = redis_options
  config.logger = Logger.new($stderr)
end
Sidekiq.configure_server do |config|
  config.redis = redis_options
  config.logger = Logger.new($stderr)
  # Use the real scheduler, including its initial random delay and retry jitter.
  config[:poll_interval_average] = 0.2
  config.error_handlers << lambda do |error, context, _config|
    job = context[:job] || context["job"] || {}
    Sidekiq.redis do |redis|
      redis.rpush("#{ENV.fetch('FFKV_SIDEKIQ_PREFIX')}:errors", JSON.generate(
        "class" => error.class.name, "jid" => job["jid"]
      ))
    end
  end
end

module FixtureResult
  def self.write(job, kind, attempt)
    prefix = ENV.fetch("FFKV_SIDEKIQ_PREFIX")
    payload = JSON.generate("kind" => kind, "attempt" => attempt,
      "jid" => job.jid, "token" => prefix, "pid" => Process.pid)
    Sidekiq.redis { |redis| redis.hset("#{prefix}:results", kind, payload) }
  end
end

class FixtureJob
  include Sidekiq::Job
  sidekiq_options retry: false

  def perform(kind)
    FixtureResult.write(self, kind, 1)
  end
end

class FixtureRetryError < StandardError; end

class FixtureRetryJob
  include Sidekiq::Job
  sidekiq_options retry: 2
  sidekiq_retry_in { |_count, _error, _job| 5 }

  def perform
    prefix = ENV.fetch("FFKV_SIDEKIQ_PREFIX")
    attempt = Sidekiq.redis { |redis| redis.incr("#{prefix}:attempts") }
    raise FixtureRetryError, "intentional first attempt" if attempt == 1

    FixtureResult.write(self, "retry", attempt)
  end
end
