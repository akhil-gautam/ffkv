Feature: Command introspection and logical database boundaries

  @command_COMMAND @command_COMMAND_INFO @command_COMMAND_COUNT @command_COMMAND_LIST
  Scenario: Discovery exposes real commands and a consistent catalog cardinality
    Given a fresh ffkv server
    When I execute:
      """
      ["COMMAND","COUNT"]
      """
    Then the reply is a positive integer
    When I save the reply as "command-count"
    When I execute:
      """
      ["COMMAND","LIST"]
      """
    Then the reply contains:
      """
      ["get","set","lpush","ft.search","fcall","cluster|slots"]
      """
    When I execute:
      """
      ["COMMAND","LIST","FILTERBY","PATTERN","ts.*"]
      """
    Then the reply contains:
      """
      ["ts.add","ts.range","ts.createrule"]
      """
    When I execute:
      """
      ["COMMAND","INFO","GET","not-a-command"]
      """
    Then the reply has JSON path "0.0" equal to:
      """
      "get"
      """
    Then the reply has JSON path "1" equal to:
      """
      null
      """
    When I execute:
      """
      ["COMMAND"]
      """
    Then the reply has the saved length "command-count"

  @command_COMMAND_DOCS
  Scenario: Versioned documentation exposes command arguments and flags
    Given a fresh ffkv server
    And client "default" uses RESP 3
    When I execute:
      """
      ["COMMAND","DOCS","GET"]
      """
    Then the reply has JSON path "get.group" equal to:
      """
      "string"
      """
    Then the reply has JSON path "get.arguments.0.name" equal to:
      """
      "key"
      """

  @command_COMMAND_GETKEYS @command_COMMAND_GETKEYSANDFLAGS
  Scenario: Counted commands expose only actual keys and directional access
    Given a fresh ffkv server
    When I execute:
      """
      ["COMMAND","GETKEYS","MSETEX","2","a","not-a-key","b","also-not-a-key","EX","20"]
      """
    Then the reply equals:
      """
      ["a","b"]
      """
    When I execute:
      """
      ["COMMAND","GETKEYS","ZMPOP","2","first","second","MIN","COUNT","3"]
      """
    Then the reply equals:
      """
      ["first","second"]
      """
    When I execute:
      """
      ["COMMAND","GETKEYSANDFLAGS","COPY","source","destination","REPLACE"]
      """
    Then the reply equals:
      """
      [["source",["RO","access"]],["destination",["OW","update"]]]
      """

  @command_DBSIZE @command_FLUSHDB @command_FLUSHALL
  Scenario: Database-local flush never removes another database
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["SET","same","zero"],["SELECT","1"],["SET","same","one"],["SET","other","value"]]
      """
    When I execute:
      """
      ["DBSIZE"]
      """
    Then the reply equals:
      """
      2
      """
    When I execute:
      """
      ["FLUSHDB","SYNC"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["GET","same"]
      """
    Then the reply is null
    Given the following commands succeed:
      """
      [["SELECT","0"]]
      """
    When I execute:
      """
      ["GET","same"]
      """
    Then the reply equals:
      """
      "zero"
      """
    Given the following commands succeed:
      """
      [["SELECT","2"],["RPUSH","list","a","b"],["SELECT","0"],["FLUSHALL","ASYNC"]]
      """
    When I execute:
      """
      ["DBSIZE"]
      """
    Then the reply equals:
      """
      0
      """
    Given the following commands succeed:
      """
      [["SELECT","2"]]
      """
    When I execute:
      """
      ["EXISTS","list"]
      """
    Then the reply equals:
      """
      0
      """
    When ffkv restarts after a crash
    Given the following commands succeed:
      """
      [["SELECT","2"]]
      """
    When I execute:
      """
      ["DBSIZE"]
      """
    Then the reply equals:
      """
      0
      """

  @command_SWAPDB
  Scenario: Swapping logical databases preserves values TTLs and recovery
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["SET","key","zero","PX","60000"],["SELECT","1"],["RPUSH","key","one","two"],["SWAPDB","0","1"]]
      """
    When I execute:
      """
      ["GET","key"]
      """
    Then the reply equals:
      """
      "zero"
      """
    When I execute:
      """
      ["PTTL","key"]
      """
    Then the reply is between 1 and 60000
    Given the following commands succeed:
      """
      [["SELECT","0"]]
      """
    When I execute:
      """
      ["LRANGE","key","0","-1"]
      """
    Then the reply equals:
      """
      ["one","two"]
      """
    When ffkv restarts after a crash
    When I execute:
      """
      ["LRANGE","key","0","-1"]
      """
    Then the reply equals:
      """
      ["one","two"]
      """

  @command_RESTORE_ASKING
  Scenario: Restore-asking uses the same verified value payload and collision rules
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["HSET","source","field","value"]]
      """
    When I execute:
      """
      ["DUMP","source"]
      """
    When I save the reply as "payload"
    When I execute:
      """
      ["RESTORE-ASKING","destination","0",{"saved":"payload"}]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["HGET","destination","field"]
      """
    Then the reply equals:
      """
      "value"
      """
    When I execute:
      """
      ["RESTORE-ASKING","destination","0",{"saved":"payload"}]
      """
    Then the reply is an error with code "BUSYKEY"
