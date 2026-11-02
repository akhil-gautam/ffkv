# Contracts: https://redis.io/docs/latest/commands/xreadgroup/; https://redis.io/docs/latest/commands/xnack/; https://redis.io/docs/latest/commands/xackdel/
Feature: Ordered stream messages consumer ownership and acknowledgement lifecycles

  @command_XADD @command_XLEN @command_XRANGE @command_XREVRANGE @command_XDEL @command_XTRIM
  Scenario: Explicit IDs preserve duplicate field ordering and range direction
    Given a fresh ffkv server
    When I execute:
      """
      ["XADD","events","1000-0","field","a","field","b"]
      """
    Then the reply equals:
      """
      "1000-0"
      """
    When I execute:
      """
      ["XADD","events","1001-0","field","c"]
      """
    Then the reply equals:
      """
      "1001-0"
      """
    When I execute:
      """
      ["XADD","events","1002-0","field","d"]
      """
    Then the reply equals:
      """
      "1002-0"
      """
    When I execute:
      """
      ["XLEN","events"]
      """
    Then the reply equals:
      """
      3
      """
    When I execute:
      """
      ["XRANGE","events","(1000-0","+"]
      """
    Then the reply equals:
      """
      [["1001-0",["field","c"]],["1002-0",["field","d"]]]
      """
    When I execute:
      """
      ["XREVRANGE","events","+","-","COUNT",2]
      """
    Then the reply equals:
      """
      [["1002-0",["field","d"]],["1001-0",["field","c"]]]
      """
    When I execute:
      """
      ["XADD","events","1001-0","field","bad"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["XDEL","events","1001-0","999-0"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["XTRIM","events","MINID","=","1002-0"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["XRANGE","events","-","+"]
      """
    Then the reply equals:
      """
      [["1002-0",["field","d"]]]
      """
    When ffkv restarts after a crash
    When I execute:
      """
      ["XRANGE","events","-","+"]
      """
    Then the reply equals:
      """
      [["1002-0",["field","d"]]]
      """
  @command_XREAD
  Scenario: A blocked read wakes with only entries newer than its requested ID
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["XADD","events","1000-0","field","a","field","b"]]
      """
    When client "reader" sends:
      """
      ["XREAD","BLOCK",0,"STREAMS","events","1000-0"]
      """
    Then client "reader" is blocked
    When I execute:
      """
      ["XADD","events","1001-0","field","c"]
      """
    Then the reply equals:
      """
      "1001-0"
      """
    Then client "reader" receives:
      """
      [["events",[["1001-0",["field","c"]]]]]
      """
    When I execute:
      """
      ["XRANGE","events","-","+"]
      """
    Then the reply equals:
      """
      [["1000-0",["field","a","field","b"]],["1001-0",["field","c"]]]
      """
  @command_XGROUP_CREATE @command_XGROUP_CREATECONSUMER @command_XREADGROUP @command_XPENDING @command_XACK @command_XINFO_CONSUMERS @command_XINFO_GROUPS @command_XINFO_STREAM
  Scenario: Groups create consumers deliver pending work and acknowledge ownership durably
    Given a fresh ffkv server
    And client "info3" uses RESP 3
    And the following commands succeed:
      """
      [["XADD","events","1000-0","field","a","field","b"],["XADD","events","1001-0","field","c"]]
      """
    When I execute:
      """
      ["XGROUP","CREATE","events","workers","0"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["XGROUP","CREATECONSUMER","events","workers","alice"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["XREADGROUP","GROUP","workers","alice","COUNT",1,"STREAMS","events",">"]
      """
    Then the reply equals:
      """
      [["events",[["1000-0",["field","a","field","b"]]]]]
      """
    When I execute:
      """
      ["XPENDING","events","workers"]
      """
    Then the reply equals:
      """
      [1,"1000-0","1000-0",[["alice","1"]]]
      """
    When client "info3" executes:
      """
      ["XINFO","CONSUMERS","events","workers"]
      """
    Then the reply contains:
      """
      [{"name":"alice","pending":1}]
      """
    When client "info3" executes:
      """
      ["XINFO","GROUPS","events"]
      """
    Then the reply contains:
      """
      [{"name":"workers","consumers":1,"pending":1,"last-delivered-id":"1000-0"}]
      """
    When client "info3" executes:
      """
      ["XINFO","STREAM","events"]
      """
    Then the reply contains:
      """
      {"length":2,"groups":1,"first-entry":["1000-0",["field","a","field","b"]],"last-entry":["1001-0",["field","c"]]}
      """
    When ffkv restarts after a crash
    When I execute:
      """
      ["XPENDING","events","workers"]
      """
    Then the reply equals:
      """
      [1,"1000-0","1000-0",[["alice","1"]]]
      """
    When I execute:
      """
      ["XACK","events","workers","1000-0"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["XPENDING","events","workers"]
      """
    Then the reply equals:
      """
      [0,null,null,null]
      """
    When I execute:
      """
      ["XRANGE","events","-","+"]
      """
    Then the reply equals:
      """
      [["1000-0",["field","a","field","b"]],["1001-0",["field","c"]]]
      """
  @command_XCLAIM @command_XAUTOCLAIM
  Scenario: Claim and autoclaim transfer ownership and preserve message payload
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["XADD","events","1000-0","field","a","field","b"],["XADD","events","1001-0","field","c"],["XGROUP","CREATE","events","workers","0"],["XREADGROUP","GROUP","workers","alice","STREAMS","events",">"]]
      """
    When I execute:
      """
      ["XCLAIM","events","workers","bob",0,"1000-0"]
      """
    Then the reply equals:
      """
      [["1000-0",["field","a","field","b"]]]
      """
    When I execute:
      """
      ["XPENDING","events","workers"]
      """
    Then the reply equals:
      """
      [2,"1000-0","1001-0",[["alice","1"],["bob","1"]]]
      """
    When I execute:
      """
      ["XAUTOCLAIM","events","workers","carol",0,"0-0","COUNT",10]
      """
    Then the reply equals:
      """
      ["0-0",[["1000-0",["field","a","field","b"]],["1001-0",["field","c"]]],[]]
      """
    When I execute:
      """
      ["XPENDING","events","workers"]
      """
    Then the reply equals:
      """
      [2,"1000-0","1001-0",[["carol","2"]]]
      """
  @command_XNACK
  Scenario: NACK releases owned work and makes it immediately claimable
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["XADD","events","1000-0","field","a","field","b"],["XGROUP","CREATE","events","workers","0"],["XREADGROUP","GROUP","workers","alice","STREAMS","events",">"]]
      """
    When I execute:
      """
      ["XNACK","events","workers","SILENT","IDS",1,"1000-0"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["XPENDING","events","workers","-","+",10]
      """
    Then the reply equals:
      """
      [["1000-0","",-1,0]]
      """
    When I execute:
      """
      ["XCLAIM","events","workers","bob",9223372036854775807,"1000-0"]
      """
    Then the reply equals:
      """
      [["1000-0",["field","a","field","b"]]]
      """
    When I execute:
      """
      ["XPENDING","events","workers"]
      """
    Then the reply equals:
      """
      [1,"1000-0","1000-0",[["bob","1"]]]
      """
  @command_XGROUP_SETID @command_XGROUP_DELCONSUMER @command_XGROUP_DESTROY
  Scenario: Group cursor reset redelivers history while consumer and group removal clear metadata
    Given a fresh ffkv server
    And client "info3" uses RESP 3
    And the following commands succeed:
      """
      [["XADD","events","1000-0","field","a","field","b"],["XADD","events","1001-0","field","c"],["XGROUP","CREATE","events","workers","$"]]
      """
    When I execute:
      """
      ["XGROUP","SETID","events","workers","0"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["XREADGROUP","GROUP","workers","alice","COUNT",1,"STREAMS","events",">"]
      """
    Then the reply equals:
      """
      [["events",[["1000-0",["field","a","field","b"]]]]]
      """
    When I execute:
      """
      ["XGROUP","DELCONSUMER","events","workers","alice"]
      """
    Then the reply equals:
      """
      1
      """
    When client "info3" executes:
      """
      ["XINFO","CONSUMERS","events","workers"]
      """
    Then the reply equals:
      """
      []
      """
    When I execute:
      """
      ["XPENDING","events","workers"]
      """
    Then the reply equals:
      """
      [0,null,null,null]
      """
    When I execute:
      """
      ["XGROUP","DESTROY","events","workers"]
      """
    Then the reply equals:
      """
      1
      """
    When client "info3" executes:
      """
      ["XINFO","GROUPS","events"]
      """
    Then the reply equals:
      """
      []
      """
    When I execute:
      """
      ["XLEN","events"]
      """
    Then the reply equals:
      """
      2
      """
  @command_XACKDEL @command_XDELEX
  Scenario: ACKED deletion waits for all group acknowledgements and DELREF clears references
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["XADD","events","1000-0","field","a","field","b"],["XGROUP","CREATE","events","a","0"],["XGROUP","CREATE","events","b","0"],["XREADGROUP","GROUP","a","alice","STREAMS","events",">"],["XREADGROUP","GROUP","b","bob","STREAMS","events",">"]]
      """
    When I execute:
      """
      ["XACKDEL","events","a","ACKED","IDS",1,"1000-0"]
      """
    Then the reply equals:
      """
      [2]
      """
    When I execute:
      """
      ["XLEN","events"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["XACKDEL","events","b","ACKED","IDS",1,"1000-0"]
      """
    Then the reply equals:
      """
      [1]
      """
    When I execute:
      """
      ["XLEN","events"]
      """
    Then the reply equals:
      """
      0
      """
    When I execute:
      """
      ["XADD","events","1001-0","field","c"]
      """
    Then the reply equals:
      """
      "1001-0"
      """
    When I execute:
      """
      ["XREADGROUP","GROUP","a","alice","STREAMS","events",">"]
      """
    Then the reply equals:
      """
      [["events",[["1001-0",["field","c"]]]]]
      """
    When I execute:
      """
      ["XDELEX","events","DELREF","IDS",1,"1001-0"]
      """
    Then the reply equals:
      """
      [1]
      """
    When I execute:
      """
      ["XPENDING","events","a"]
      """
    Then the reply equals:
      """
      [0,null,null,null]
      """
    When I execute:
      """
      ["XLEN","events"]
      """
    Then the reply equals:
      """
      0
      """
  @command_XSETID
  Scenario: Replicated stream IDs govern subsequent insertion and expose counters
    Given a fresh ffkv server
    And client "info3" uses RESP 3
    And the following commands succeed:
      """
      [["XADD","events","1000-0","field","a","field","b"]]
      """
    When I execute:
      """
      ["XSETID","events","2000-0","ENTRIESADDED",7,"MAXDELETEDID","999-0"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "info3" executes:
      """
      ["XINFO","STREAM","events"]
      """
    Then the reply contains:
      """
      {"last-generated-id":"2000-0","entries-added":7,"max-deleted-entry-id":"999-0"}
      """
    When I execute:
      """
      ["XADD","events","1500-0","field","invalid"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["XADD","events","2000-1","field","next"]
      """
    Then the reply equals:
      """
      "2000-1"
      """
    When I execute:
      """
      ["XLEN","events"]
      """
    Then the reply equals:
      """
      2
      """
  @command_XCFGSET @command_XIDMPRECORD
  Scenario: Idempotency replay retains original payload and configuration reset permits a fresh append
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["XADD","events","1000-0","field","a","field","b"],["XADD","events","1001-0","field","c"]]
      """
    When I execute:
      """
      ["XCFGSET","events","IDMP-DURATION",300,"IDMP-MAXSIZE",2]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["XIDMPRECORD","events","producer","order-1","1000-0"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["XIDMPRECORD","events","producer","order-1","1000-0"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["XADD","events","IDMP","producer","order-1","*","field","must-not-replace"]
      """
    Then the reply equals:
      """
      "1000-0"
      """
    When I execute:
      """
      ["XLEN","events"]
      """
    Then the reply equals:
      """
      2
      """
    When I execute:
      """
      ["XRANGE","events","1000-0","1000-0"]
      """
    Then the reply equals:
      """
      [["1000-0",["field","a","field","b"]]]
      """
    When I execute:
      """
      ["XIDMPRECORD","events","producer","order-1","1001-0"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["XADD","events","IDMP","producer","order-1","*","field","still-not-replaced"]
      """
    Then the reply equals:
      """
      "1000-0"
      """
    When ffkv restarts after a crash
    When I execute:
      """
      ["XADD","events","IDMP","producer","order-1","*","field","after-recovery"]
      """
    Then the reply equals:
      """
      "1000-0"
      """
    When I execute:
      """
      ["XCFGSET","events","IDMP-MAXSIZE",3]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["XADD","events","IDMP","producer","order-1","*","field","new-after-reset"]
      """
    When I save the reply as "new-id"
    When I execute:
      """
      ["XLEN","events"]
      """
    Then the reply equals:
      """
      3
      """
    When I execute:
      """
      ["XDEL","events",{"saved":"new-id"}]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["XLEN","events"]
      """
    Then the reply equals:
      """
      2
      """
    When I execute:
      """
      ["XRANGE","events","1000-0","1000-0"]
      """
    Then the reply equals:
      """
      [["1000-0",["field","a","field","b"]]]
      """

  @command_XTRIM @command_XACK @command_XPENDING
  Scenario Outline: ACKED trimming skips pending entries rather than stopping at them
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["XADD","events","1-0","f","a"],["XADD","events","2-0","f","b"],["XGROUP","CREATE","events","g","0"],["XREADGROUP","GROUP","g","c","STREAMS","events",">"],["XACK","events","g","2-0"]]
      """
    When I execute:
      """
      ["XTRIM","events","<strategy>","=",<threshold>,"ACKED"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["XRANGE","events","-","+"]
      """
    Then the reply equals:
      """
      [["1-0",["f","a"]]]
      """
    When I execute:
      """
      ["XPENDING","events","g"]
      """
    Then the reply equals:
      """
      [1,"1-0","1-0",[["c","1"]]]
      """
    When ffkv restarts after a crash
    When I execute:
      """
      ["XRANGE","events","-","+"]
      """
    Then the reply equals:
      """
      [["1-0",["f","a"]]]
      """
    When I execute:
      """
      ["XPENDING","events","g"]
      """
    Then the reply equals:
      """
      [1,"1-0","1-0",[["c","1"]]]
      """

    Examples:
      | strategy | threshold |
      | MAXLEN   | 0         |
      | MINID    | "3-0"     |

  @command_XADD @command_XTRIM @command_XPENDING
  Scenario Outline: Repeated reference policies reject the whole command without deleting entries or pending ownership
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["XADD","events","1-0","f","a"],["XADD","events","2-0","f","b"],["XGROUP","CREATE","events","g","0"],["XREADGROUP","GROUP","g","c","STREAMS","events",">"]]
      """
    When I execute:
      """
      <command>
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["XRANGE","events","-","+"]
      """
    Then the reply equals:
      """
      [["1-0",["f","a"]],["2-0",["f","b"]]]
      """
    When I execute:
      """
      ["XPENDING","events","g"]
      """
    Then the reply equals:
      """
      [2,"1-0","2-0",[["c","2"]]]
      """
    When ffkv restarts after a crash
    When I execute:
      """
      ["XRANGE","events","-","+"]
      """
    Then the reply equals:
      """
      [["1-0",["f","a"]],["2-0",["f","b"]]]
      """
    When I execute:
      """
      ["XPENDING","events","g","-","+",10]
      """
    Then the reply has 2 items
    And the reply has JSON path "0.0" equal to:
      """
      "1-0"
      """
    And the reply has JSON path "1.0" equal to:
      """
      "2-0"
      """
    And the reply has JSON path "0.1" equal to:
      """
      "c"
      """
    And the reply has JSON path "1.1" equal to:
      """
      "c"
      """
    And the reply has JSON path "0.3" equal to:
      """
      1
      """
    And the reply has JSON path "1.3" equal to:
      """
      1
      """

    Examples:
      | command                                                                   |
      | ["XTRIM","events","KEEPREF","MAXLEN",0,"DELREF"]                             |
      | ["XTRIM","events","DELREF","MAXLEN",0,"DELREF"]                              |
      | ["XTRIM","events","ACKED","MAXLEN",0,"KEEPREF"]                              |
      | ["XTRIM","events","MAXLEN",0,"KEEPREF","KEEPREF"]                            |
      | ["XADD","events","KEEPREF","DELREF","MAXLEN",0,"3-0","f","new"]              |
      | ["XADD","events","KEEPREF","MAXLEN",0,"DELREF","3-0","f","new"]              |
      | ["XADD","events","MAXLEN",0,"DELREF","DELREF","3-0","f","new"]               |
      | ["XADD","events","ACKED","MAXLEN",0,"ACKED","3-0","f","new"]                |

  @command_XCLAIM @command_XPENDING
  Scenario Outline: FORCE counts both creation and claim unless JUSTID or RETRYCOUNT changes the count
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["XADD","events","1-0","f","a"],["XGROUP","CREATE","events","g","0"]]
      """
    When I execute:
      """
      ["XCLAIM","events","g","c",0,"1-0","FORCE"<options>]
      """
    Then the reply equals:
      """
      <claimed>
      """
    When I execute:
      """
      ["XPENDING","events","g","-","+",10]
      """
    Then the reply has 1 items
    And the reply has JSON path "0.0" equal to:
      """
      "1-0"
      """
    And the reply has JSON path "0.1" equal to:
      """
      "c"
      """
    And the reply has JSON path "0.3" equal to:
      """
      <count>
      """
    When ffkv restarts after a crash
    When I execute:
      """
      ["XRANGE","events","-","+"]
      """
    Then the reply equals:
      """
      [["1-0",["f","a"]]]
      """
    When I execute:
      """
      ["XPENDING","events","g","-","+",10]
      """
    Then the reply has 1 items
    And the reply has JSON path "0.0" equal to:
      """
      "1-0"
      """
    And the reply has JSON path "0.1" equal to:
      """
      "c"
      """
    And the reply has JSON path "0.3" equal to:
      """
      <count>
      """

    Examples:
      | options                       | count | claimed                 |
      |                               | 2     | [["1-0",["f","a"]]]      |
      | ,"JUSTID"                     | 1     | ["1-0"]                 |
      | ,"RETRYCOUNT",0               | 0     | [["1-0",["f","a"]]]      |
      | ,"RETRYCOUNT",7               | 7     | [["1-0",["f","a"]]]      |
      | ,"JUSTID","RETRYCOUNT",0      | 0     | ["1-0"]                 |
      | ,"RETRYCOUNT",7,"JUSTID"      | 7     | ["1-0"]                 |
