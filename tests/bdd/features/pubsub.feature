Feature: Persistent native clients exchange regular and shard publications

  @command_SUBSCRIBE @command_PUBLISH @command_UNSUBSCRIBE
  Scenario: A regular subscription delivers payloads and unsubscribe removes the recipient
    Given a fresh ffkv server
    When client "listener" sends:
      """
      ["SUBSCRIBE","orders"]
      """
    Then client "listener" receives:
      """
      ["subscribe","orders",1]
      """
    When I execute:
      """
      ["PUBLISH","orders","created"]
      """
    Then the reply equals:
      """
      1
      """
    Then client "listener" receives:
      """
      ["message","orders","created"]
      """
    When client "listener" sends:
      """
      ["UNSUBSCRIBE","orders"]
      """
    Then client "listener" receives:
      """
      ["unsubscribe","orders",0]
      """
    When I execute:
      """
      ["PUBLISH","orders","cancelled"]
      """
    Then the reply equals:
      """
      0
      """
    When client "listener" executes:
      """
      ["PING"]
      """
    Then the reply equals:
      """
      "PONG"
      """

  @command_PSUBSCRIBE @command_PUNSUBSCRIBE @command_PUBSUB_NUMPAT
  Scenario: Patterns are unique in inspection but each matching registration receives its own message
    Given a fresh ffkv server
    When client "one" sends:
      """
      ["PSUBSCRIBE","order:*"]
      """
    Then client "one" receives:
      """
      ["psubscribe","order:*",1]
      """
    When client "two" sends:
      """
      ["PSUBSCRIBE","order:*"]
      """
    Then client "two" receives:
      """
      ["psubscribe","order:*",1]
      """
    When I execute:
      """
      ["PUBSUB","NUMPAT"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["PUBLISH","order:7","paid"]
      """
    Then the reply equals:
      """
      2
      """
    Then client "one" receives:
      """
      ["pmessage","order:*","order:7","paid"]
      """
    Then client "two" receives:
      """
      ["pmessage","order:*","order:7","paid"]
      """
    When client "one" sends:
      """
      ["PUNSUBSCRIBE"]
      """
    Then client "one" receives:
      """
      ["punsubscribe","order:*",0]
      """
    When client "two" sends:
      """
      ["PUNSUBSCRIBE","order:*"]
      """
    Then client "two" receives:
      """
      ["punsubscribe","order:*",0]
      """
    When I execute:
      """
      ["PUBSUB","NUMPAT"]
      """
    Then the reply equals:
      """
      0
      """

  @command_PUBSUB_CHANNELS @command_PUBSUB_NUMSUB
  Scenario: Regular channel inspection applies glob filtering and counts exact subscribers
    Given a fresh ffkv server
    When client "listener" sends:
      """
      ["SUBSCRIBE","order:7","news"]
      """
    Then client "listener" receives:
      """
      ["subscribe","order:7",1]
      """
    Then client "listener" receives:
      """
      ["subscribe","news",2]
      """
    When I execute:
      """
      ["PUBSUB","CHANNELS","order:*"]
      """
    Then the reply equals:
      """
      ["order:7"]
      """
    When I execute:
      """
      ["PUBSUB","NUMSUB","order:7","news","absent"]
      """
    Then the reply equals:
      """
      ["order:7",1,"news",1,"absent",0]
      """

  @command_SSUBSCRIBE @command_SPUBLISH @command_SUNSUBSCRIBE @command_PUBSUB_SHARDCHANNELS @command_PUBSUB_SHARDNUMSUB
  Scenario: Shard channels are disjoint from regular channels even when their names match
    Given a fresh ffkv server
    When client "regular" sends:
      """
      ["SUBSCRIBE","{orders}:events"]
      """
    Then client "regular" receives:
      """
      ["subscribe","{orders}:events",1]
      """
    When client "shard" sends:
      """
      ["SSUBSCRIBE","{orders}:events"]
      """
    Then client "shard" receives:
      """
      ["ssubscribe","{orders}:events",1]
      """
    When I execute:
      """
      ["PUBSUB","SHARDCHANNELS","{orders}:*"]
      """
    Then the reply equals:
      """
      ["{orders}:events"]
      """
    When I execute:
      """
      ["PUBSUB","SHARDNUMSUB","{orders}:events","absent"]
      """
    Then the reply equals:
      """
      ["{orders}:events",1,"absent",0]
      """
    When I execute:
      """
      ["SPUBLISH","{orders}:events","sharded"]
      """
    Then the reply equals:
      """
      1
      """
    Then client "shard" receives:
      """
      ["smessage","{orders}:events","sharded"]
      """
    Then client "regular" has no reply for 50 milliseconds
    When I execute:
      """
      ["PUBLISH","{orders}:events","ordinary"]
      """
    Then the reply equals:
      """
      1
      """
    Then client "regular" receives:
      """
      ["message","{orders}:events","ordinary"]
      """
    Then client "shard" has no reply for 50 milliseconds
    When client "shard" sends:
      """
      ["SUNSUBSCRIBE"]
      """
    Then client "shard" receives:
      """
      ["sunsubscribe","{orders}:events",0]
      """
    When I execute:
      """
      ["SPUBLISH","{orders}:events","after-unsubscribe"]
      """
    Then the reply equals:
      """
      0
      """

  @command_SUBSCRIBE @command_PUBLISH @command_UNSUBSCRIBE @command_SSUBSCRIBE @command_SPUBLISH @command_SUNSUBSCRIBE
  Scenario: RESP3 subscription pushes do not prevent ordinary commands on the same connection
    Given a fresh ffkv server
    And client "listener" uses RESP 3
    When client "listener" sends:
      """
      ["SUBSCRIBE","news"]
      """
    Then client "listener" receives:
      """
      {"type":"push","values":["subscribe","news",1]}
      """
    When client "listener" executes:
      """
      ["SET","outside","usable"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "listener" sends:
      """
      ["SSUBSCRIBE","news"]
      """
    Then client "listener" receives:
      """
      {"type":"push","values":["ssubscribe","news",1]}
      """
    When I execute:
      """
      ["SPUBLISH","news","shard-payload"]
      """
    Then the reply equals:
      """
      1
      """
    Then client "listener" receives:
      """
      {"type":"push","values":["smessage","news","shard-payload"]}
      """
    When client "listener" sends:
      """
      ["SUNSUBSCRIBE","news"]
      """
    Then client "listener" receives:
      """
      {"type":"push","values":["sunsubscribe","news",0]}
      """
    When client "listener" sends:
      """
      ["UNSUBSCRIBE","news"]
      """
    Then client "listener" receives:
      """
      {"type":"push","values":["unsubscribe","news",0]}
      """

  @command_SPUBLISH @command_SSUBSCRIBE @command_PUBLISH @command_SUBSCRIBE
  Scenario: Real cluster peer links forward shard publications to replicas and global publications across shards
    Given a fresh ffkv cluster with 3 primaries and 3 replicas
    When client "shard-replica" on node "node4" sends:
      """
      ["SSUBSCRIBE","{{node1_key}}"]
      """
    Then client "shard-replica" receives:
      """
      ["ssubscribe","{{node1_key}}",1]
      """
    When client "other-shard" on node "node2" sends:
      """
      ["SUBSCRIBE","{{node1_key}}"]
      """
    Then client "other-shard" receives:
      """
      ["subscribe","{{node1_key}}",1]
      """
    When node "node1" executes:
      """
      ["SPUBLISH","{{node1_key}}","shard-peer-payload"]
      """
    Then the reply equals:
      """
      0
      """
    Then client "shard-replica" receives:
      """
      ["smessage","{{node1_key}}","shard-peer-payload"]
      """
    Then client "other-shard" has no reply for 50 milliseconds
    Then client "shard-replica" has no reply for 50 milliseconds
    When node "node1" executes:
      """
      ["PUBLISH","{{node1_key}}","global-peer-payload"]
      """
    Then the reply equals:
      """
      0
      """
    Then client "other-shard" receives:
      """
      ["message","{{node1_key}}","global-peer-payload"]
      """
    Then client "shard-replica" has no reply for 50 milliseconds
