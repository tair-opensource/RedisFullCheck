# frozen_string_literal: true

# value repair script
require "io/console"
require "redis"
require "redis-clustering"
require "sqlite3"
require "benchmark"

SLICE_SIZE = 10
PATH_TO_SQL_DB = "../../RedisFullCheck/result.db.3"

p "source redis endpoint:"
source_redis_endpoint = STDIN.noecho(&:gets).chomp

p "target memorydb endpoint:"
target_memorydb_endpoint = STDIN.noecho(&:gets).chomp

p "dryrun? (y/n)"
dryrun = gets.chomp == "y"

source_elasticache = Redis.new(url: source_redis_endpoint)
target_memdb = Redis::Cluster.new(nodes: [target_memorydb_endpoint] ,reconnect_attempts: 3, timeout: 0.1, slow_command_timeout: 0.1)

# call RedisFullCheck
# cmd = `../RedisFullCheck/bin/redis-full-check -s #{source_redis_endpoint} -p #{source_redis_pwd} -t #{targete_redis_endpoint} -a #{target_redis_auth} --targetdbtype=1 --comparemode=2 --comparetimes=3`

Benchmark.bm do |benchmark|

  db = SQLite3::Database.open PATH_TO_SQL_DB
  db.results_as_hash = true

  # 1. repair string values
  benchmark.report("Repair Strings") do
    results = db.query "SELECT key where type = 'string' from key"

    results.each_slice(SLICE_SIZE) do |slice|
      keys = slice.each {|r| r["key"]}
      elasticache_vals = source_elasticache.mget(*keys)
      # zip and flatten so in the order of 'k1', 'v1', 'k2', 'v2'
      mset_args = keys.zip(elasticache_vals).flatten
      # set memdb value as
      memdb.mset(mset_args) unless dryrun
    end
  end

  # 2. repair set values
  benchmark.report("Repair Values") do
    set_results = db.query "SELECT key where type = set from key"

    set_results.each do |row|
      k = row["key"]
      set_members = source_elasticache.smembers(k)
      # remove and re-add the set members to the k
      unless dryrun
        memdb.multi do |multi|
          multi.del(k) 
          multi.sadd(k, set_members)
        end
      end
    end
  end

  # 3. repair set values
  benchmark.report("Repair Sets") do
    hash_results = db.query "SELECT key where type = hash from key"
    hash_results.each do |row|
      k = row["key"]
      hval = elasticache.hgetall(k)
      # set memdb value as the hash value of k
      unless dryrun
        memdb.multi do |multi|
          multi.del(k) 
          multi.hset(k, hval)
        end
      end
    end
  end
end
