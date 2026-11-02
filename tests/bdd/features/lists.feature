Feature: Redis 8.10 lists preserve ordering, ownership, and atomicity
  # Contracts: https://redis.io/docs/latest/commands/lmovem/
  # https://redis.io/docs/latest/commands/blmovem/
  # https://redis.io/docs/latest/commands/lpos/
  # Pinned behavioral reference (independently implemented):
  # https://github.com/redis/redis/blob/5279a8d44818a5ca51e9abb91a9b8ce481d3c88b/tests/unit/type/list-4.tcl

  @command_LPUSH @command_RPUSH @command_LRANGE @command_LLEN
  Scenario: Multi-element pushes have different argument ordering at each end
    Given a fresh ffkv server
    When I execute:
      """
      ["LPUSH","q","a","b","c"]
      """
    Then the reply equals:
      """
      3
      """
    When I execute:
      """
      ["RPUSH","q","d","e"]
      """
    Then the reply equals:
      """
      5
      """
    When I execute:
      """
      ["LRANGE","q","0","-1"]
      """
    Then the reply equals:
      """
      ["c","b","a","d","e"]
      """
    When I execute:
      """
      ["LLEN","q"]
      """
    Then the reply equals:
      """
      5
      """

  @command_LPUSHX @command_RPUSHX
  Scenario: Conditional pushes never create missing lists and retain argument order
    Given a fresh ffkv server
    When I execute:
      """
      ["LPUSHX","missing","a","b"]
      """
    Then the reply equals:
      """
      0
      """
    When I execute:
      """
      ["RPUSHX","missing","a","b"]
      """
    Then the reply equals:
      """
      0
      """
    Given the following commands succeed:
      """
      [["RPUSH","q","middle"]]
      """
    When I execute:
      """
      ["LPUSHX","q","a","b"]
      """
    Then the reply equals:
      """
      3
      """
    When I execute:
      """
      ["RPUSHX","q","c","d"]
      """
    Then the reply equals:
      """
      5
      """
    When I execute:
      """
      ["LRANGE","q","0","-1"]
      """
    Then the reply equals:
      """
      ["b","a","middle","c","d"]
      """

  @command_LPOP @command_RPOP
  Scenario: Counted pops return traversal order and delete emptied keys
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["RPUSH","q","a","b","c","d","e"]]
      """
    When I execute:
      """
      ["LPOP","q","2"]
      """
    Then the reply equals:
      """
      ["a","b"]
      """
    When I execute:
      """
      ["RPOP","q","9"]
      """
    Then the reply equals:
      """
      ["e","d","c"]
      """
    When I execute:
      """
      ["EXISTS","q"]
      """
    Then the reply equals:
      """
      0
      """
    When I execute:
      """
      ["LPOP","q"]
      """
    Then the reply is null

  @command_LPOP @command_RPOP
  Scenario Outline: Zero counts do not consume and negative counts fail atomically
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["RPUSH","q","a","b"]]
      """
    When I execute:
      """
      ["<command>","q","0"]
      """
    Then the reply equals:
      """
      []
      """
    When I execute:
      """
      ["<command>","q","-1"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["LRANGE","q","0","-1"]
      """
    Then the reply equals:
      """
      ["a","b"]
      """
    Examples:
      | command |
      | LPOP    |
      | RPOP    |

  @command_LINDEX @command_LSET @command_LTRIM
  Scenario: Negative indexes and inclusive trim endpoints preserve list order
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["RPUSH","q","a","b","c","d"]]
      """
    When I execute:
      """
      ["LINDEX","q","-2"]
      """
    Then the reply equals:
      """
      "c"
      """
    When I execute:
      """
      ["LSET","q","-2","C"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["LSET","q","4","bad"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["LTRIM","q","1","-2"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["LRANGE","q","-9223372036854775808","9223372036854775807"]
      """
    Then the reply equals:
      """
      ["b","C"]
      """
    When I execute:
      """
      ["LINDEX","q","-3"]
      """
    Then the reply is null
    Given the following commands succeed:
      """
      [["LTRIM","q","3","2"]]
      """
    When I execute:
      """
      ["EXISTS","q"]
      """
    Then the reply equals:
      """
      0
      """

  @command_LREM
  Scenario: Removal counts select from the requested end without disturbing survivors
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["RPUSH","q","a","x","a","y","a"]]
      """
    When I execute:
      """
      ["LREM","q","-1","a"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["LRANGE","q","0","-1"]
      """
    Then the reply equals:
      """
      ["a","x","a","y"]
      """
    When I execute:
      """
      ["LREM","q","1","a"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["LREM","q","0","a"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["LRANGE","q","0","-1"]
      """
    Then the reply equals:
      """
      ["x","y"]
      """

  @command_LINSERT
  Scenario: Insertion resolves the first pivot and reports missing pivots without mutation
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["RPUSH","q","a","pivot","b","pivot"]]
      """
    When I execute:
      """
      ["LINSERT","q","BEFORE","pivot","before"]
      """
    Then the reply equals:
      """
      5
      """
    When I execute:
      """
      ["LINSERT","q","AFTER","pivot","after"]
      """
    Then the reply equals:
      """
      6
      """
    When I execute:
      """
      ["LINSERT","q","BEFORE","absent","bad"]
      """
    Then the reply equals:
      """
      -1
      """
    When I execute:
      """
      ["LINSERT","missing","BEFORE","pivot","bad"]
      """
    Then the reply equals:
      """
      0
      """
    When I execute:
      """
      ["LINSERT","q","SIDEWAYS","pivot","bad"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["LRANGE","q","0","-1"]
      """
    Then the reply equals:
      """
      ["a","before","pivot","after","b","pivot"]
      """

  @command_LPOS
  Scenario: Positions combine negative rank, count, maxlen, and stable forward indexes
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["RPUSH","q","a","x","b","x","x","c"]]
      """
    When I execute:
      """
      ["LPOS","q","x"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["LPOS","q","x","RANK","-2","COUNT","0","MAXLEN","5"]
      """
    Then the reply equals:
      """
      [3,1]
      """
    When I execute:
      """
      ["LPOS","q","x","RANK","2","COUNT","1","MAXLEN","3"]
      """
    Then the reply equals:
      """
      []
      """
    When I execute:
      """
      ["LPOS","q","absent"]
      """
    Then the reply is null
    When I execute:
      """
      ["LPOS","q","x","RANK","0"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["LPOS","q","x","COUNT","-1"]
      """
    Then the reply is an error with code "ERR"

  @command_LMPOP
  Scenario: Multi-pop honors key order, duplicates, direction, and the chosen key only
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["RPUSH","first","a","b","c"],["RPUSH","second","x","y"],["SET","wrong","string"]]
      """
    When I execute:
      """
      ["LMPOP","4","missing","first","first","wrong","RIGHT","COUNT","2"]
      """
    Then the reply equals:
      """
      ["first",["c","b"]]
      """
    When I execute:
      """
      ["LRANGE","first","0","-1"]
      """
    Then the reply equals:
      """
      ["a"]
      """
    When I execute:
      """
      ["LMPOP","2","wrong","second","LEFT"]
      """
    Then the reply is an error with code "WRONGTYPE"
    When I execute:
      """
      ["LMPOP","1","second","LEFT","COUNT","0"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["LRANGE","second","0","-1"]
      """
    Then the reply equals:
      """
      ["x","y"]
      """
    When I execute:
      """
      ["LMPOP","1","missing","LEFT"]
      """
    Then the reply is null

  @command_LMOVE
  Scenario Outline: Single moves implement all endpoint combinations
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["RPUSH","source","a","b","c"],["RPUSH","destination","x"]]
      """
    When I execute:
      """
      ["LMOVE","source","destination","<from>","<to>"]
      """
    Then the reply equals:
      """
      "<moved>"
      """
    When I execute:
      """
      ["LRANGE","destination","0","-1"]
      """
    Then the reply equals:
      """
      <destination>
      """
    Examples:
      | from  | to    | moved | destination |
      | LEFT  | LEFT  | a     | ["a","x"]   |
      | LEFT  | RIGHT | a     | ["x","a"]   |
      | RIGHT | LEFT  | c     | ["c","x"]   |
      | RIGHT | RIGHT | c     | ["x","c"]   |

  @command_RPOPLPUSH
  Scenario: Legacy right-to-left move rotates a same-key list and preserves its expiry
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["RPUSH","q","a","b","c"],["PEXPIREAT","q","4102444800000"]]
      """
    When I execute:
      """
      ["RPOPLPUSH","q","q"]
      """
    Then the reply equals:
      """
      "c"
      """
    When I execute:
      """
      ["LRANGE","q","0","-1"]
      """
    Then the reply equals:
      """
      ["c","a","b"]
      """
    When I execute:
      """
      ["PEXPIRETIME","q"]
      """
    Then the reply equals:
      """
      4102444800000
      """
    Given the following commands succeed:
      """
      [["LTRIM","q","0","0"],["RPOPLPUSH","q","q"]]
      """
    When I execute:
      """
      ["PEXPIRETIME","q"]
      """
    Then the reply equals:
      """
      4102444800000
      """

  @command_LMOVE @command_RPOPLPUSH @command_LMOVEM
  Scenario Outline: Destination type errors cannot consume the source
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["RPUSH","source","a","b"],["SET","destination","string"]]
      """
    When I execute:
      """
      <command>
      """
    Then the reply is an error with code "WRONGTYPE"
    When I execute:
      """
      ["LRANGE","source","0","-1"]
      """
    Then the reply equals:
      """
      ["a","b"]
      """
    Examples:
      | command                                                        |
      | ["LMOVE","source","destination","LEFT","RIGHT"]              |
      | ["RPOPLPUSH","source","destination"]                            |
      | ["LMOVEM","source","destination","RIGHT","LEFT","COUNT","2","OBO"] |

  @command_LMOVEM
  Scenario Outline: Batch movement returns destination order for all endpoints and modes
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["RPUSH","source","a","b","c","d"],["RPUSH","destination","x"]]
      """
    When I execute:
      """
      ["LMOVEM","source","destination","<from>","<to>","COUNT","2","<mode>"]
      """
    Then the reply equals:
      """
      <moved>
      """
    When I execute:
      """
      ["LRANGE","destination","0","-1"]
      """
    Then the reply equals:
      """
      <destination>
      """
    Examples:
      | from  | to    | mode | moved     | destination     |
      | LEFT  | LEFT  | OBO  | ["b","a"] | ["b","a","x"]   |
      | LEFT  | LEFT  | BULK | ["a","b"] | ["a","b","x"]   |
      | LEFT  | RIGHT | OBO  | ["a","b"] | ["x","a","b"]   |
      | LEFT  | RIGHT | BULK | ["a","b"] | ["x","a","b"]   |
      | RIGHT | LEFT  | OBO  | ["c","d"] | ["c","d","x"]   |
      | RIGHT | LEFT  | BULK | ["c","d"] | ["c","d","x"]   |
      | RIGHT | RIGHT | OBO  | ["d","c"] | ["x","d","c"]   |
      | RIGHT | RIGHT | BULK | ["c","d"] | ["x","c","d"]   |

  @command_LMOVEM
  Scenario Outline: Same-key batch selection happens before any push back
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["RPUSH","q","a","b","c","d"]]
      """
    When I execute:
      """
      ["LMOVEM","q","q","<from>","<to>","EXACTLY","2","<mode>"]
      """
    Then the reply equals:
      """
      <moved>
      """
    When I execute:
      """
      ["LRANGE","q","0","-1"]
      """
    Then the reply equals:
      """
      <remaining>
      """
    Examples:
      | from  | to    | mode | moved     | remaining         |
      | LEFT  | LEFT  | OBO  | ["b","a"] | ["b","a","c","d"] |
      | LEFT  | LEFT  | BULK | ["a","b"] | ["a","b","c","d"] |
      | RIGHT | RIGHT | OBO  | ["d","c"] | ["a","b","d","c"] |
      | RIGHT | RIGHT | BULK | ["c","d"] | ["a","b","c","d"] |
      | LEFT  | RIGHT | OBO  | ["a","b"] | ["c","d","a","b"] |
      | RIGHT | LEFT  | BULK | ["c","d"] | ["c","d","a","b"] |

  @command_LMOVEM
  Scenario: Exactly is all-or-nothing and count bounds the available source
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["RPUSH","source","a","b"]]
      """
    When I execute:
      """
      ["LMOVEM","source","destination","LEFT","RIGHT","EXACTLY","3","BULK"]
      """
    Then the reply is null
    When I execute:
      """
      ["LRANGE","source","0","-1"]
      """
    Then the reply equals:
      """
      ["a","b"]
      """
    When I execute:
      """
      ["LMOVEM","source","destination","LEFT","RIGHT","COUNT","9","BULK"]
      """
    Then the reply equals:
      """
      ["a","b"]
      """
    When I execute:
      """
      ["EXISTS","source"]
      """
    Then the reply equals:
      """
      0
      """
    When I execute:
      """
      ["LMOVEM","missing","destination","LEFT","RIGHT"]
      """
    Then the reply equals:
      """
      []
      """
    When I execute:
      """
      ["LMOVEM","destination","missing","LEFT","RIGHT","COUNT","1"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["LRANGE","destination","0","-1"]
      """
    Then the reply equals:
      """
      ["a","b"]
      """

  @command_BLPOP @command_BRPOP
  Scenario Outline: Blocking single pops choose the first available key and traverse its selected end
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["RPUSH","q","a","b"],["SET","wrong","string"]]
      """
    When I execute:
      """
      ["<command>","missing","q","wrong","0"]
      """
    Then the reply equals:
      """
      ["q","<value>"]
      """
    Examples:
      | command | value |
      | BLPOP   | a     |
      | BRPOP   | b     |

  @command_BLMOVE @command_BRPOPLPUSH
  Scenario Outline: Blocking moves wake on source availability and insert at the destination end
    Given a fresh ffkv server
    When client "consumer" sends:
      """
      <command>
      """
    Then client "consumer" is blocked
    When I execute:
      """
      ["RPUSH","source","a","b"]
      """
    Then the reply equals:
      """
      2
      """
    Then client "consumer" receives:
      """
      "b"
      """
    When I execute:
      """
      ["LRANGE","destination","0","-1"]
      """
    Then the reply equals:
      """
      ["b"]
      """
    Examples:
      | command                                               |
      | ["BLMOVE","source","destination","RIGHT","LEFT","0"] |
      | ["BRPOPLPUSH","source","destination","0"]              |

  @command_BLMPOP
  Scenario: A blocked multi-pop consumes a batch from the first newly available list
    Given a fresh ffkv server
    When client "consumer" sends:
      """
      ["BLMPOP","0","2","first","second","RIGHT","COUNT","2"]
      """
    Then client "consumer" is blocked
    When I execute:
      """
      ["RPUSH","second","a","b","c"]
      """
    Then the reply equals:
      """
      3
      """
    Then client "consumer" receives:
      """
      ["second",["c","b"]]
      """
    When I execute:
      """
      ["LRANGE","second","0","-1"]
      """
    Then the reply equals:
      """
      ["a"]
      """

  @command_BLMOVEM
  Scenario: Exactly blocking movement waits for the entire batch and preserves binary payloads
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["RPUSH","source",{"base64":"AP+A"}]]
      """
    When client "consumer" sends:
      """
      ["BLMOVEM","source","destination","LEFT","LEFT","0","EXACTLY","2","OBO"]
      """
    Then client "consumer" is blocked
    When I execute:
      """
      ["LRANGE","source","0","-1"]
      """
    Then the reply equals:
      """
      [{"type":"bytes","base64":"AP+A"}]
      """
    When I execute:
      """
      ["RPUSH","source","last"]
      """
    Then the reply equals:
      """
      2
      """
    Then client "consumer" receives:
      """
      ["last",{"type":"bytes","base64":"AP+A"}]
      """
    When I execute:
      """
      ["EXISTS","source"]
      """
    Then the reply equals:
      """
      0
      """

  @command_BLMOVEM
  Scenario: Count blocking movement wakes with fewer than the requested count
    Given a fresh ffkv server
    When client "consumer" sends:
      """
      ["BLMOVEM","source","destination","RIGHT","RIGHT","0","COUNT","5","BULK"]
      """
    Then client "consumer" is blocked
    Given the following commands succeed:
      """
      [["RPUSH","source","a","b"]]
      """
    Then client "consumer" receives:
      """
      ["a","b"]
      """

  @command_BLPOP @command_BRPOP @command_BLMPOP @command_BLMOVEM
  Scenario: Blocked consumers have FIFO priority and cancellation removes a waiter
    Given a fresh ffkv server
    When client "cancelled" sends:
      """
      ["BLPOP","q","0"]
      """
    Then client "cancelled" is blocked
    When client "cancelled" disconnects
    When client "first" sends:
      """
      ["BRPOP","q","0"]
      """
    Then client "first" is blocked
    When client "second" sends:
      """
      ["BLMPOP","0","1","q","LEFT","COUNT","2"]
      """
    Then client "second" is blocked
    When client "third" sends:
      """
      ["BLMOVEM","q","destination","LEFT","RIGHT","0","EXACTLY","2","BULK"]
      """
    Then client "third" is blocked
    Given the following commands succeed:
      """
      [["RPUSH","q","a","b","c","d","e"]]
      """
    Then client "first" receives:
      """
      ["q","e"]
      """
    Then client "second" receives:
      """
      ["q",["a","b"]]
      """
    Then client "third" receives:
      """
      ["c","d"]
      """
    When I execute:
      """
      ["LRANGE","destination","0","-1"]
      """
    Then the reply equals:
      """
      ["c","d"]
      """

  @command_BLPOP @command_BRPOP @command_BLMOVE @command_BRPOPLPUSH @command_BLMPOP @command_BLMOVEM
  Scenario Outline: Finite blocking timeouts return null without creating destination lists
    Given a fresh ffkv server
    When I execute:
      """
      <command>
      """
    Then the reply is null
    When I execute:
      """
      ["EXISTS","source","destination"]
      """
    Then the reply equals:
      """
      0
      """
    Examples:
      | command                                                                                   |
      | ["BLPOP","source","0.01"]                                                                |
      | ["BRPOP","source","0.01"]                                                                |
      | ["BLMOVE","source","destination","LEFT","RIGHT","0.01"]                                  |
      | ["BRPOPLPUSH","source","destination","0.01"]                                              |
      | ["BLMPOP","0.01","1","source","LEFT","COUNT","2"]                                        |
      | ["BLMOVEM","source","destination","LEFT","RIGHT","0.01","EXACTLY","2","BULK"]              |

  @command_BLPOP @command_BRPOP @command_BLMOVE @command_BRPOPLPUSH @command_BLMPOP @command_BLMOVEM
  Scenario: Blocking operations inside EXEC return immediately and later writes are not consumed
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["MULTI"],["BLPOP","source","0"],["BRPOP","source","0"],["BLMOVE","source","destination","LEFT","RIGHT","0"],["BRPOPLPUSH","source","destination","0"],["BLMPOP","0","1","source","LEFT"],["BLMOVEM","source","destination","LEFT","RIGHT","0","EXACTLY","2","BULK"],["RPUSH","source","a","b"]]
      """
    When I execute:
      """
      ["EXEC"]
      """
    Then the reply equals:
      """
      [null,null,null,null,null,null,2]
      """
    When I execute:
      """
      ["LRANGE","source","0","-1"]
      """
    Then the reply equals:
      """
      ["a","b"]
      """

  @command_LINSERT @command_LMOVEM @command_RPOPLPUSH
  Scenario: Resolved insertion and batch movement recover with ordering and binary values intact
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["RPUSH","source","a",{"base64":"AP+A"},"c"],["LINSERT","source","AFTER","a","middle"],["LMOVEM","source","destination","LEFT","LEFT","EXACTLY","3","OBO"],["RPOPLPUSH","destination","destination"]]
      """
    When ffkv restarts after a crash
    When I execute:
      """
      ["LRANGE","source","0","-1"]
      """
    Then the reply equals:
      """
      ["c"]
      """
    When I execute:
      """
      ["LRANGE","destination","0","-1"]
      """
    Then the reply equals:
      """
      ["a",{"type":"bytes","base64":"AP+A"},"middle"]
      """

  @command_BLMOVEM @command_BLMPOP
  Scenario: Blocked requests do not survive a server lifecycle but completed effects do
    Given a fresh ffkv server
    When client "old" sends:
      """
      ["BLMPOP","0","1","source","LEFT"]
      """
    Then client "old" is blocked
    When ffkv restarts after a crash
    Given the following commands succeed:
      """
      [["RPUSH","source","a","b"]]
      """
    When I execute:
      """
      ["BLMOVEM","source","destination","LEFT","RIGHT","0","EXACTLY","2","BULK"]
      """
    Then the reply equals:
      """
      ["a","b"]
      """
    When ffkv restarts after a crash
    When I execute:
      """
      ["LRANGE","destination","0","-1"]
      """
    Then the reply equals:
      """
      ["a","b"]
      """

  @command_LINSERT @command_BLMOVEM
  Scenario: An insertion wakes an exact batch after an earlier insufficient push
    Given a fresh ffkv server
    When client "consumer" sends:
      """
      ["BLMOVEM","source","destination","LEFT","RIGHT","0","EXACTLY","3","BULK"]
      """
    Then client "consumer" is blocked
    Given the following commands succeed:
      """
      [["RPUSH","source","a","c"]]
      """
    Then client "consumer" is blocked
    When I execute:
      """
      ["LRANGE","source","0","-1"]
      """
    Then the reply equals:
      """
      ["a","c"]
      """
    When I execute:
      """
      ["EXISTS","destination"]
      """
    Then the reply equals:
      """
      0
      """
    When I execute:
      """
      ["LINSERT","source","BEFORE","c","b"]
      """
    Then the reply equals:
      """
      3
      """
    Then client "consumer" receives:
      """
      ["a","b","c"]
      """

  @command_BLMOVEM
  Scenario: An exact batch timeout leaves a short source untouched
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["RPUSH","source","a"]]
      """
    When I execute:
      """
      ["BLMOVEM","source","destination","LEFT","RIGHT","0.01","EXACTLY","2","BULK"]
      """
    Then the reply is null
    When I execute:
      """
      ["LRANGE","source","0","-1"]
      """
    Then the reply equals:
      """
      ["a"]
      """
    When I execute:
      """
      ["EXISTS","destination"]
      """
    Then the reply equals:
      """
      0
      """

  @command_LMPOP @command_LMOVEM @command_LPOS
  Scenario: RESP3 keeps nested pop arrays, destination-order move arrays, and missing positions distinct
    Given a fresh ffkv server
    And client "resp3" uses RESP 3
    And the following commands succeed:
      """
      [["RPUSH","source","a","b","c"]]
      """
    When client "resp3" executes:
      """
      ["LMPOP","1","source","RIGHT","COUNT","1"]
      """
    Then the reply equals:
      """
      ["source",["c"]]
      """
    When client "resp3" executes:
      """
      ["LMOVEM","source","destination","LEFT","LEFT","COUNT","2","OBO"]
      """
    Then the reply equals:
      """
      ["b","a"]
      """
    When client "resp3" executes:
      """
      ["LPOS","destination","missing"]
      """
    Then the reply is null
    When client "resp3" executes:
      """
      ["LPOS","destination","missing","COUNT","0"]
      """
    Then the reply equals:
      """
      []
      """

  @command_BLMOVE @command_BLMOVEM @command_BLMPOP @command_BLPOP @command_BRPOP @command_BRPOPLPUSH @command_LINDEX @command_LINSERT @command_LLEN @command_LMOVE @command_LMOVEM @command_LMPOP @command_LPOP @command_LPOS @command_LPUSH @command_LPUSHX @command_LRANGE @command_LREM @command_LSET @command_LTRIM @command_RPOP @command_RPOPLPUSH @command_RPUSH @command_RPUSHX
  Scenario Outline: List commands reject incompatible source values without replacing them
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["SET","wrong","untouched"]]
      """
    When I execute:
      """
      <command>
      """
    Then the reply is an error with code "WRONGTYPE"
    When I execute:
      """
      ["GET","wrong"]
      """
    Then the reply equals:
      """
      "untouched"
      """
    Examples:
      | command                                                       |
      | ["BLMOVE","wrong","destination","LEFT","RIGHT","0"]             |
      | ["BLMOVEM","wrong","destination","LEFT","RIGHT","0"]            |
      | ["BLMPOP","0","1","wrong","LEFT"]                               |
      | ["BLPOP","wrong","0"]                                          |
      | ["BRPOP","wrong","0"]                                          |
      | ["BRPOPLPUSH","wrong","destination","0"]                        |
      | ["LINDEX","wrong","0"]                                         |
      | ["LINSERT","wrong","BEFORE","pivot","value"]                    |
      | ["LLEN","wrong"]                                               |
      | ["LMOVE","wrong","destination","LEFT","RIGHT"]                  |
      | ["LMOVEM","wrong","destination","LEFT","RIGHT"]                 |
      | ["LMPOP","1","wrong","LEFT"]                                    |
      | ["LPOP","wrong"]                                               |
      | ["LPOS","wrong","value"]                                       |
      | ["LPUSH","wrong","value"]                                      |
      | ["LPUSHX","wrong","value"]                                     |
      | ["LRANGE","wrong","0","-1"]                                    |
      | ["LREM","wrong","0","value"]                                   |
      | ["LSET","wrong","0","value"]                                   |
      | ["LTRIM","wrong","0","-1"]                                     |
      | ["RPOP","wrong"]                                               |
      | ["RPOPLPUSH","wrong","destination"]                            |
      | ["RPUSH","wrong","value"]                                      |
      | ["RPUSHX","wrong","value"]                                     |
