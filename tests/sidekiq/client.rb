# frozen_string_literal: true

require_relative "jobs"
require "sidekiq/api"
require "redis_client"

prefix = ENV.fetch("FFKV_SIDEKIQ_PREFIX")
queue = ENV.fetch("FFKV_SIDEKIQ_QUEUE")
held_queue = "#{queue}_held"
redis = RedisClient.config(url: ENV.fetch("FFKV_SIDEKIQ_URL"), timeout: 2).new_client

def hash_reply(value)
  value.is_a?(Hash) ? value : value.each_slice(2).to_h
end

begin
  output = case ARGV.fetch(0)
  when "seed"
    raise "fixture endpoint is not empty" unless redis.call("DBSIZE") == 0

    {
      "immediate" => FixtureJob.set(queue: queue).perform_async("immediate"),
      "scheduled" => FixtureJob.set(queue: queue).perform_in(3, "scheduled"),
      "retry" => FixtureRetryJob.set(queue: queue).perform_async,
      "held" => FixtureJob.set(queue: held_queue).perform_async("held_queue_marker"),
      "future" => FixtureJob.set(queue: queue).perform_in(3600, "future_schedule_marker")
    }
  when "snapshot"
    stats = Sidekiq::Stats.new
    {
      "versions" => {
        "ruby" => RUBY_VERSION, "sidekiq" => Sidekiq::VERSION,
        "redis-client" => Gem.loaded_specs.fetch("redis-client").version.to_s
      },
      "results" => hash_reply(redis.call("HGETALL", "#{prefix}:results"))
        .transform_values { |value| JSON.parse(value) },
      "attempts" => redis.call("GET", "#{prefix}:attempts").to_i,
      "errors" => redis.call("LRANGE", "#{prefix}:errors", 0, -1).map { |v| JSON.parse(v) },
      "queues" => Sidekiq::Queue.all.to_h { |q| [q.name, q.size] },
      "scheduled" => Sidekiq::ScheduledSet.new.map(&:item),
      "retries" => Sidekiq::RetrySet.new.map(&:item),
      "processes" => Sidekiq::ProcessSet.new.map do |process|
        %w[identity pid tag concurrency queues beat quiet busy version].to_h { |k| [k, process[k]] }
      end,
      "stats" => {
        "processed" => stats.processed, "failed" => stats.failed,
        "enqueued" => stats.enqueued, "scheduled" => stats.scheduled_size,
        "retries" => stats.retry_size, "dead" => stats.dead_size
      }
    }
  else
    raise ArgumentError, "unknown fixture action"
  end
  puts JSON.generate(output)
ensure
  redis.close
end
