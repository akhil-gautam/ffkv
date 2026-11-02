Feature: Administrative commands share their real runtime inside transactions
  Runtime controls borrow the transaction's engine pause instead of requesting another one.

  @command_MULTI @command_EXEC @command_CONFIG_SET @command_CONFIG_GET @command_MEMORY_USAGE @command_INFO
  Scenario: Transactional configuration publishes effective values and observations do not repause engines
    Given a fresh ffkv server
    And client "admin" uses RESP 3
    When client "admin" executes:
      """
      ["MULTI"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "admin" executes:
      """
      ["CONFIG","SET","maxclients","4096","slowlog-max-len","31"]
      """
    Then the reply equals:
      """
      "QUEUED"
      """
    When client "admin" executes:
      """
      ["CONFIG","GET","maxclients","slowlog-max-len"]
      """
    Then the reply equals:
      """
      "QUEUED"
      """
    When client "admin" executes:
      """
      ["MEMORY","USAGE","missing:transaction:key","SAMPLES","0"]
      """
    Then the reply equals:
      """
      "QUEUED"
      """
    When client "admin" executes:
      """
      ["INFO","not-a-real-section"]
      """
    Then the reply equals:
      """
      "QUEUED"
      """
    When client "admin" executes:
      """
      ["EXEC"]
      """
    Then the reply equals:
      """
      ["OK",{"maxclients":"4096","slowlog-max-len":"31"},null,""]
      """
    When client "admin" executes:
      """
      ["CONFIG","GET","maxclients"]
      """
    Then the reply contains:
      """
      {"maxclients":"4096"}
      """

  @command_SAVE @command_CONFIG_SET @command_EXEC
  Scenario: AOF configuration inside EXEC preserves the saved committed prefix exactly once
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["CONFIG","SET","appendonly","no"],["RPUSH","transaction:journal","before-transaction"],["SAVE"],["MULTI"],["RPUSH","transaction:journal","before-config"],["CONFIG","SET","appendonly","yes"],["RPUSH","transaction:journal","after-config"]]
      """
    When I execute:
      """
      ["EXEC"]
      """
    Then the reply equals:
      """
      [2,"OK",3]
      """
    When ffkv restarts after a crash
    When I execute:
      """
      ["LRANGE","transaction:journal",0,-1]
      """
    Then the reply equals:
      """
      ["before-transaction","before-config","after-config"]
      """

  @command_CONFIG_RESETSTAT @command_LATENCY_HISTOGRAM @command_EXEC
  Scenario: A transactional statistics reset clears real prior observations without clearing data
    Given a fresh ffkv server
    And client "admin" uses RESP 3
    And the following commands succeed:
      """
      [["SET","transaction:statistics","retained"],["GET","transaction:statistics"]]
      """
    When client "admin" executes:
      """
      ["MULTI"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "admin" executes:
      """
      ["CONFIG","RESETSTAT"]
      """
    Then the reply equals:
      """
      "QUEUED"
      """
    When client "admin" executes:
      """
      ["LATENCY","HISTOGRAM","get"]
      """
    Then the reply equals:
      """
      "QUEUED"
      """
    When client "admin" executes:
      """
      ["GET","transaction:statistics"]
      """
    Then the reply equals:
      """
      "QUEUED"
      """
    When client "admin" executes:
      """
      ["EXEC"]
      """
    Then the reply equals:
      """
      ["OK",{},"retained"]
      """

  @command_BGSAVE @command_EXEC @command_INFO
  Scenario: A background save scheduled by EXEC runs after its inherited pause is released
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["MULTI"],["SET","transaction:background","saved"],["BGSAVE"]]
      """
    When I execute:
      """
      ["EXEC"]
      """
    Then the reply equals:
      """
      ["OK","Background saving started"]
      """
    When I wait until executing:
      """
      {"command":["INFO","persistence"],"text_contains":"rdb_bgsave_in_progress:0"}
      """
    When ffkv restarts after a crash
    When I execute:
      """
      ["GET","transaction:background"]
      """
    Then the reply equals:
      """
      "saved"
      """

  @command_SELECT @command_MEMORY_USAGE @command_EXEC
  Scenario: Memory usage inside EXEC observes the database selected by a preceding queued command
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["SET","transaction:database","only-in-zero"],["MULTI"],["SELECT","1"],["MEMORY","USAGE","transaction:database"],["SELECT","0"],["GET","transaction:database"]]
      """
    When I execute:
      """
      ["EXEC"]
      """
    Then the reply equals:
      """
      ["OK",null,"OK","only-in-zero"]
      """

  @command_BGSAVE @command_SHUTDOWN @command_EXEC
  Scenario: Graceful shutdown drains a save and its scheduled successor from EXEC
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["MULTI"],["RPUSH","transaction:draining","durable"],["BGSAVE"],["BGSAVE","SCHEDULE"]]
      """
    When I execute:
      """
      ["EXEC"]
      """
    Then the reply equals:
      """
      [1,"Background saving started","Background saving scheduled"]
      """
    When ffkv shuts down with SAVE
    When ffkv starts again
    When I execute:
      """
      ["LRANGE","transaction:draining",0,-1]
      """
    Then the reply equals:
      """
      ["durable"]
      """

  @command_MEMORY_STATS @command_INFO @command_EXEC
  Scenario: Memory statistics distinguish allocated bytes from actual live keys across databases
    Given a fresh ffkv server
    And client "memory" uses RESP 3
    And the following commands succeed:
      """
      [["SET","memory:large","this payload is deliberately much larger than either the number of keys or a tiny metadata counter"],["RPUSH","memory:deleted","one","two"],["SELECT","1"],["SET","memory:other-database","retained"],["SELECT","0"]]
      """
    When client "memory" executes:
      """
      ["MULTI"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "memory" executes:
      """
      ["DEL","memory:deleted"]
      """
    Then the reply equals:
      """
      "QUEUED"
      """
    When client "memory" executes:
      """
      ["MEMORY","STATS"]
      """
    Then the reply equals:
      """
      "QUEUED"
      """
    When client "memory" executes:
      """
      ["EXEC"]
      """
    Then the reply has JSON path "0" equal to:
      """
      1
      """
    And the reply has JSON path "1.keys.count" equal to:
      """
      2
      """
    And the reply has JSON path "1.total.allocated" with an integer greater than 32
    When I execute:
      """
      ["INFO","memory"]
      """
    Then the reply text contains "ffkv_dataset_keys:2"
    When client "memory" executes:
      """
      ["MEMORY","STATS"]
      """
    Then the reply has JSON path "keys.count" equal to:
      """
      2
      """
    And the reply has JSON path "total.allocated" with an integer greater than 32

  @command_MULTI @command_EXEC @command_EVAL @command_ACL_SETUSER
  Scenario: Queue admission checks command ACLs even when script key counts are malformed
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["ACL","SETUSER","queue-user","on","nopass","+multi","+exec","+set","~allowed:*"]]
      """
    When client "queue-user" executes:
      """
      ["AUTH","queue-user","unused"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "queue-user" executes:
      """
      ["MULTI"]
      """
    When client "queue-user" executes:
      """
      ["SET","allowed:before","discarded"]
      """
    Then the reply equals:
      """
      "QUEUED"
      """
    When client "queue-user" executes:
      """
      ["EVAL","return 1",-1]
      """
    Then the reply is an error with code "NOPERM"
    When client "queue-user" executes:
      """
      ["EXEC"]
      """
    Then the reply is an error with code "EXECABORT"
    When I execute:
      """
      ["GET","allowed:before"]
      """
    Then the reply is null

  @command_MULTI @command_EXEC @command_EVAL @command_ACL_SETUSER
  Scenario: Script key ACLs are checked at queue admission and rechecked during execution
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["SET","allowed:item","private"],["ACL","SETUSER","queue-user","on","nopass","+multi","+exec","+eval","+get","~allowed:*"]]
      """
    When client "queue-user" executes:
      """
      ["AUTH","queue-user","unused"]
      """
    When client "queue-user" executes:
      """
      ["MULTI"]
      """
    When client "queue-user" executes:
      """
      ["EVAL","return redis.call('GET',KEYS[1])",1,"forbidden:item"]
      """
    Then the reply is an error with code "NOPERM"
    When client "queue-user" executes:
      """
      ["EXEC"]
      """
    Then the reply is an error with code "EXECABORT"
    When client "queue-user" executes:
      """
      ["MULTI"]
      """
    When client "queue-user" executes:
      """
      ["EVAL","return redis.call('GET',KEYS[1])",1,"allowed:item"]
      """
    Then the reply equals:
      """
      "QUEUED"
      """
    When I execute:
      """
      ["ACL","SETUSER","queue-user","resetkeys","~other:*"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "queue-user" executes:
      """
      ["EXEC"]
      """
    Then the EXEC reply contains an error with code "NOPERM"
