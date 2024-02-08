require 'io/console'
require "redis"
require "redis-clustering"

p "source redis endpoint:"
source_redis_endpoint = STDIN.noecho(&:gets).chomp

p "target memorydb endpoint:"
target_memorydb_endpoint = STDIN.noecho(&:gets).chomp

p "dryrun? (y/n)"
dryrun = gets.chomp == "y"

source_elasticache = Redis.new(url: source_redis_endpoint)
target_memdb = Redis::Cluster.new(nodes: [target_memorydb_endpoint] ,reconnect_attempts: 3, timeout: 0.1, slow_command_timeout: 0.1)

cursor = 0
updated_keys = []
scanned_keys = 0

loop do
  p "START LOOP - cursor: #{cursor}"
  cursor, keys = target_memdb.scan(cursor, count: 100, type: "string")

  # bulk get TTL of keys from memdb
  memdb_ttl_values = target_memdb.pipelined do |pipelined|
    keys.each {|k| pipelined.ttl(k)}
  end

  # bulk get TTL / expiretime of keys from elasticache
  e_ttl_values = source_elasticache.pipelined do |pipelined|
    keys.each {|k| pipelined.ttl(k)}
  end

  # bulk update expiretime for any differences
  target_memdb.pipelined do |pipelined|
    memdb_ttl_values.each_with_index do |memdb_ttl_val, i|
      e_val = e_ttl_values[i]
      if (memdb_ttl_val != e_val) && (e_val != -2)
        p "ttl values do not match!"
        pipelined.expire(keys[i], e_val) unless dryrun
        updated_keys << { key: keys[i], old_value: memdb_ttl_val, new_value: e_val }
      end
    end
  end

  scanned_keys += keys.size
  p "now scanned: #{scanned_keys}!\n"

  break if cursor == "0"
end

p ""
p "FINISHED"
p "number of keys scanned: #{scanned_keys}"
p "number of key TTLs updated if not dryrun: #{updated_keys.size}"
p updated_keys