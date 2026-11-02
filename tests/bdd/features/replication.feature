@native_replication
Feature: Native replication preserves byte history and truthful durability acknowledgements
  All nodes, proxy links, snapshot files, sockets and fault gates belong to the
  current scenario. No running development replica or external service is used.

  @command_REPLICAOF
  Scenario: Initial synchronization replaces stale data and streams later writes
    Given an owned native replication pair not yet following
    When node "master" executes:
      """
      ["SET","before-sync","snapshot-value"]
      """
    When node "replica" executes:
      """
      ["SET","stale-only","discard-me"]
      """
    When node "replica" executes:
      """
      ["REPLICAOF","{{master_host}}","{{master_port}}"]
      """
    Then the reply equals:
      """
      "OK"
      """
    Then node "replica" eventually replies:
      """
      {"command":["GET","before-sync"],"reply":"snapshot-value"}
      """
    When node "replica" executes:
      """
      ["GET","stale-only"]
      """
    Then the reply is null
    When node "master" executes:
      """
      ["SET","after-sync","live-value"]
      """
    Then node "replica" eventually replies:
      """
      {"command":["GET","after-sync"],"reply":"live-value"}
      """
    When node "replica" executes:
      """
      ["SET","forbidden","write"]
      """
    Then the reply is an error with code "READONLY"

  @command_REPLICAOF @command_SLAVEOF
  Scenario Outline: Explicit promotion retains data and accepts new writes
    Given an owned synchronized native replication pair
    When node "master" executes:
      """
      ["SET","retained","before-promotion"]
      """
    Then node "replica" eventually replies:
      """
      {"command":["GET","retained"],"reply":"before-promotion"}
      """
    When node "replica" executes:
      """
      ["<command>","NO","ONE"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When node "replica" executes:
      """
      ["SET","retained","after-promotion"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When node "replica" executes:
      """
      ["GET","retained"]
      """
    Then the reply equals:
      """
      "after-promotion"
      """
    Examples:
      | command   |
      | REPLICAOF |
      | SLAVEOF   |

  @command_SLAVEOF
  Scenario: The catalogued legacy spelling starts real followership
    Given an owned native replication pair not yet following
    When node "master" executes:
      """
      ["SET","legacy-source","visible"]
      """
    When node "replica" executes:
      """
      ["SLAVEOF","{{master_host}}","{{master_port}}"]
      """
    Then the reply equals:
      """
      "OK"
      """
    Then node "replica" eventually replies:
      """
      {"command":["GET","legacy-source"],"reply":"visible"}
      """

  @command_PSYNC
  Scenario: Reconnection requests exactly the missing bytes without another snapshot
    Given an owned synchronized native replication pair
    When the owned replication link is disconnected
    When node "master" executes:
      """
      ["SET","while-offline","recovered"]
      """
    When node "master" executes:
      """
      ["LPUSH","random-source","one","two","three"]
      """
    When node "master" executes:
      """
      ["RPOPLPUSH","random-source","resolved-destination"]
      """
    When the owned replication link is reconnected
    Then the next replica synchronization is partial
    And the resumed replication bytes equal the missing primary history bytes
    Then node "replica" eventually replies:
      """
      {"command":["GET","while-offline"],"reply":"recovered"}
      """
    Then node "replica" eventually replies:
      """
      {"command":["LRANGE","resolved-destination","0","-1"],"reply":["one"]}
      """

  @command_PSYNC
  Scenario: Wrong history and evicted history transfer a valid bounded RDB
    Given an owned synchronized native replication pair with a 1024 byte backlog
    Given native probe "wrong-history" requests PSYNC with an unknown history
    Then native probe "wrong-history" receives a FULLRESYNC header and a valid Redis RDB
    When the owned replication link is disconnected
    And owned writes exceed the primary replication backlog
    When the owned replication link is reconnected
    Then the next replica synchronization is full
    And the replica dataset equals the primary dataset

  @command_SYNC
  Scenario: Legacy synchronization consumes real RDB then live RESP effects
    Given an owned synchronized native replication pair
    Given native probe "legacy" sends SYNC to node "master"
    Then native probe "legacy" receives a valid Redis RDB without a FULLRESYNC header
    When node "master" executes:
      """
      ["SET","legacy-stream","live"]
      """
    Then native probe "legacy" applies the stream through key "legacy-stream" with value "live"
    When native probe "legacy" reconnects using SYNC
    Then native probe "legacy" receives a fresh valid Redis RDB without a FULLRESYNC header

  @command_REPLCONF
  Scenario: Capability negotiation ignores unknown capabilities but not unknown options
    Given a fresh ffkv server
    When I execute:
      """
      ["REPLCONF","listening-port","12345","capa","eof","capa","psync2","capa","future-capability"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["REPLCONF","not-an-option","value"]
      """
    Then the reply is an error with code "ERR"

  @command_REPLCONF @command_WAIT @command_WAITAOF
  Scenario: Processed ACK does not pretend the replica has fsynced the write
    Given an owned synchronized native replication pair
    When the owned replica fsync gate is closed
    When node "master" executes:
      """
      ["SET","durability-boundary","written"]
      """
    When node "master" executes:
      """
      ["WAIT","1","2000"]
      """
    Then the reply equals:
      """
      1
      """
    When node "master" executes:
      """
      ["WAITAOF","1","1","20"]
      """
    Then the reply equals:
      """
      [1,0]
      """
    When the owned replica fsync gate is opened
    When node "master" executes:
      """
      ["WAITAOF","1","1","2000"]
      """
    Then the reply equals:
      """
      [1,1]
      """
    When the owned replica restarts after a crash
    Then node "replica" eventually replies:
      """
      {"command":["GET","durability-boundary"],"reply":"written"}
      """

  @command_WAIT
  Scenario: Wait uses this connection's watermark rather than another writer's offset
    Given an owned synchronized native replication pair
    When client "early" on node "master" executes:
      """
      ["SET","early-key","acknowledged"]
      """
    When client "early" on node "master" executes:
      """
      ["WAIT","1","2000"]
      """
    Then the reply equals:
      """
      1
      """
    When the owned replication stream is paused without disconnecting ACK traffic
    When client "late" on node "master" executes:
      """
      ["SET","late-key","not-yet-processed"]
      """
    When client "early" on node "master" executes:
      """
      ["WAIT","1","20"]
      """
    Then the reply equals:
      """
      1
      """
    When client "late" on node "master" executes:
      """
      ["WAIT","1","20"]
      """
    Then the reply equals:
      """
      0
      """
    When the owned replication stream is resumed
    When client "late" on node "master" executes:
      """
      ["WAIT","1","2000"]
      """
    Then the reply equals:
      """
      1
      """

  @command_WAIT @command_WAITAOF
  Scenario: Disconnected replicas are not counted and replicas reject wait commands
    Given an owned synchronized native replication pair
    When the owned replication link is disconnected
    When node "master" executes:
      """
      ["SET","offline-write","pending"]
      """
    When node "master" executes:
      """
      ["WAIT","1","20"]
      """
    Then the reply equals:
      """
      0
      """
    When node "master" executes:
      """
      ["WAITAOF","0","1","20"]
      """
    Then the reply equals:
      """
      [1,0]
      """
    When node "replica" executes:
      """
      ["WAIT","0","1"]
      """
    Then the reply is an error with code "ERR"
    When node "replica" executes:
      """
      ["WAITAOF","0","0","1"]
      """
    Then the reply is an error with code "ERR"

  @command_FAILOVER
  Scenario: A coordinated standalone handoff preserves writes and changes both roles
    Given an owned synchronized native replication pair
    When node "master" executes:
      """
      ["SET","handoff-key","retained"]
      """
    When node "master" executes:
      """
      ["FAILOVER","TO","{{replica_host}}","{{replica_port}}","TIMEOUT","2000"]
      """
    Then the reply equals:
      """
      "OK"
      """
    Then node "replica" eventually has replication role "master"
    And node "master" eventually follows node "replica"
    When node "replica" executes:
      """
      ["SET","handoff-key","new-primary"]
      """
    Then node "master" eventually replies:
      """
      {"command":["GET","handoff-key"],"reply":"new-primary"}
      """

  @command_FAILOVER
  Scenario: Timeout and explicit abort remove the write pause without promoting a stale replica
    Given an owned synchronized native replication pair
    When the owned replication stream is paused without disconnecting ACK traffic
    When node "master" executes:
      """
      ["SET","unprocessed","value"]
      """
    When node "master" executes:
      """
      ["FAILOVER","TIMEOUT","20"]
      """
    Then the reply equals:
      """
      "OK"
      """
    Then node "master" eventually accepts writes after failover timeout
    When node "master" executes:
      """
      ["FAILOVER","TIMEOUT","5000"]
      """
    When node "master" executes:
      """
      ["FAILOVER","ABORT"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When node "master" executes:
      """
      ["SET","after-abort","writable"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When node "master" executes:
      """
      ["FAILOVER","FORCE"]
      """
    Then the reply is an error with code "ERR"

  @command_REPLICAOF @command_PSYNC
  Scenario: Protected-network refusal does not leak upstream credentials or install data
    Given an owned native replication pair with protected upstream networking
    When the replica is configured with credentials for a protected-network upstream
    Then no connection or credential bytes reach the protected-network upstream
    And node "replica" has no connected native upstream
    When node "replica" executes:
      """
      ["GET","protected-local-only"]
      """
    Then the reply is an error with code "LOADING"
    When node "replica" executes:
      """
      ["REPLICAOF","NO","ONE"]
      """
    Then the reply equals:
      """
      "OK"
      """
    And node "replica" eventually has replication role "master"
    And its preexisting dataset is unchanged

  @command_FAILOVER
  Scenario: An implicit target performs the same coordinated role swap
    Given an owned synchronized native replication pair
    When node "master" executes:
      """
      ["FAILOVER","TIMEOUT","2000"]
      """
    Then the reply equals:
      """
      "OK"
      """
    Then node "replica" eventually has replication role "master"
    And node "master" eventually follows node "replica"

  @command_FAILOVER
  Scenario: Explicit forced handoff can promote an owned target after the catch-up deadline
    Given an owned synchronized native replication pair
    When the owned replication stream is paused without disconnecting ACK traffic
    When node "master" executes:
      """
      ["SET","not-acknowledged","force-does-not-promise-to-retain-this"]
      """
    When node "master" executes:
      """
      ["FAILOVER","TO","{{replica_host}}","{{replica_port}}","FORCE","TIMEOUT","20"]
      """
    Then the reply equals:
      """
      "OK"
      """
    Then node "replica" eventually has replication role "master"
    And node "master" eventually follows node "replica"
    When node "replica" executes:
      """
      ["SET","after-force","accepted"]
      """
    Then the reply equals:
      """
      "OK"
      """
    Then node "master" eventually replies:
      """
      {"command":["GET","after-force"],"reply":"accepted"}
      """

  @command_WAITAOF
  Scenario: Disabled AOF and impossible local thresholds fail without changing data
    Given an owned native replication pair with primary AOF disabled
    When node "master" executes:
      """
      ["SET","aof-disabled","retained"]
      """
    When node "master" executes:
      """
      ["WAITAOF","1","0","1"]
      """
    Then the reply is an error with code "ERR"
    When node "master" executes:
      """
      ["WAITAOF","2","0","1"]
      """
    Then the reply is an error with code "ERR"
    When node "master" executes:
      """
      ["GET","aof-disabled"]
      """
    Then the reply equals:
      """
      "retained"
      """

  @command_MODULE_LOAD @command_COMMAND_GETKEYS @command_COMMAND_LIST
  Scenario: Full polling image replacement retains local command registry ownership
    Given an owned HTTP snapshot-polling replication pair
    When node "polling-source" executes:
      """
      ["SET","polling:marker","base"]
      """
    Then node "polling-follower" eventually replies:
      """
      {"command":["GET","polling:marker"],"equals":"base"}
      """
    When node "polling-follower" executes:
      """
      ["MODULE","LOAD","{{module_fixture}}"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When node "polling-follower" executes:
      """
      ["COMMAND","GETKEYS","fixture.keyset","polling:local","value"]
      """
    Then the reply equals:
      """
      ["polling:local"]
      """
    When node "polling-source" executes:
      """
      ["SET","polling:marker","replacement"]
      """
    Then node "polling-follower" eventually replies:
      """
      {"command":["GET","polling:marker"],"equals":"replacement"}
      """
    When node "polling-follower" executes:
      """
      ["COMMAND","LIST","FILTERBY","MODULE","fixture"]
      """
    Then the reply contains:
      """
      ["fixture.keyset","fixture.get"]
      """
    When node "polling-follower" executes:
      """
      ["COMMAND","GETKEYS","fixture.keyset","polling:local","value"]
      """
    Then the reply equals:
      """
      ["polling:local"]
      """
