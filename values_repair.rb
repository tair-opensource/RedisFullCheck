# value repair script
require 'io/console'
require "redis"
require "redis-clustering"
require "sqlite3"

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

db = SQLite3::Database.open "../RedisFullCheck/result.db.3"
db.results_as_hash = true

# 1. repair string values
results = db.query "SELECT key where type = 'string' from key"

results.each_slice(10) do |slice|
  keys = slice.each {|r| r["key"]}
  elasticache_vals = source_elasticache.mget(*keys)
  # zip and flatten so in the order of 'k1', 'v1', 'k2', 'v2'
  mset_args = keys.zip(elasticache_vals).flatten
  # set memdb value as
  memdb.mset(mset_args) unless dryrun
end

# 2. repair set values
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

hash_results = db.query "SELECT key where type = hash from key"

# 3. repair set values
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
