Feature: Redis 8.10 sets and counted set algebra
  # Contracts: https://redis.io/docs/latest/commands/sdiffcard/ and sunioncard/.

@command_SADD @command_SCARD @command_SREM
Scenario: Membership changes count distinct members and remove empty keys
  Given a fresh ffkv server
  When I execute:
    """
    ["SADD","s","a","a","b"]
    """
  Then the reply equals:
    """
    2
    """
  When I execute:
    """
    ["SCARD","s"]
    """
  Then the reply equals:
    """
    2
    """
  When I execute:
    """
    ["SREM","s","a","a","missing"]
    """
  Then the reply equals:
    """
    1
    """
  When I execute:
    """
    ["SREM","s","b"]
    """
  Then the reply equals:
    """
    1
    """
  When I execute:
    """
    ["EXISTS","s"]
    """
  Then the reply equals:
    """
    0
    """
  When I execute:
    """
    ["SCARD","missing"]
    """
  Then the reply equals:
    """
    0
    """

@command_SISMEMBER @command_SMISMEMBER
Scenario: Membership queries preserve duplicate query positions
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["SADD","s","a","b"]]
    """
  When I execute:
    """
    ["SISMEMBER","s","a"]
    """
  Then the reply equals:
    """
    1
    """
  When I execute:
    """
    ["SISMEMBER","s","z"]
    """
  Then the reply equals:
    """
    0
    """
  When I execute:
    """
    ["SMISMEMBER","s","a","z","a"]
    """
  Then the reply equals:
    """
    [1,0,1]
    """
  When I execute:
    """
    ["SMISMEMBER","missing","a","z"]
    """
  Then the reply equals:
    """
    [0,0]
    """

@command_SMEMBERS
Scenario: Set replies contain each distinct member once
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["SADD","s","only","only"]]
    """
  Given client "reader" uses RESP 3
  When client "reader" executes:
    """
    ["SMEMBERS","s"]
    """
  Then the reply equals:
    """
    {"type":"set","values":["only"]}
    """
  When client "reader" executes:
    """
    ["SMEMBERS","missing"]
    """
  Then the reply equals:
    """
    {"type":"set","values":[]}
    """

@command_SDIFF
Scenario: SDIFF respects overlapping and missing sources
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["SADD","a","a","b"],["SADD","b","b","c"]]
    """
  Given client "reader" uses RESP 3
  When client "reader" executes:
    """
    ["SDIFF","a","b"]
    """
  Then the reply equals:
    """
    {"type":"set","values":["a"]}
    """
  When client "reader" executes:
    """
    ["SDIFF","missing","missing"]
    """
  Then the reply equals:
    """
    {"type":"set","values":[]}
    """

@command_SINTER
Scenario: SINTER respects overlapping and missing sources
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["SADD","a","a","b"],["SADD","b","b","c"]]
    """
  Given client "reader" uses RESP 3
  When client "reader" executes:
    """
    ["SINTER","a","b"]
    """
  Then the reply equals:
    """
    {"type":"set","values":["b"]}
    """
  When client "reader" executes:
    """
    ["SINTER","missing","missing"]
    """
  Then the reply equals:
    """
    {"type":"set","values":[]}
    """

@command_SUNION
Scenario: SUNION respects overlapping and missing sources
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["SADD","a","only"],["SADD","b","only"]]
    """
  Given client "reader" uses RESP 3
  When client "reader" executes:
    """
    ["SUNION","a","b"]
    """
  Then the reply equals:
    """
    {"type":"set","values":["only"]}
    """
  When client "reader" executes:
    """
    ["SUNION","missing","missing"]
    """
  Then the reply equals:
    """
    {"type":"set","values":[]}
    """

@command_SDIFFSTORE
Scenario: SDIFFSTORE can overwrite an input after computing the result
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["SADD","a","a","b"],["SADD","b","b","c"]]
    """
  When I execute:
    """
    ["SDIFFSTORE","a","a","b"]
    """
  Then the reply equals:
    """
    1
    """
  When I execute:
    """
    ["SISMEMBER","a","a"]
    """
  Then the reply equals:
    """
    1
    """
  When I execute:
    """
    ["SDIFFSTORE","a","missing"]
    """
  Then the reply equals:
    """
    0
    """
  When I execute:
    """
    ["EXISTS","a"]
    """
  Then the reply equals:
    """
    0
    """

@command_SINTERSTORE
Scenario: SINTERSTORE can overwrite an input after computing the result
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["SADD","a","a","b"],["SADD","b","b","c"]]
    """
  When I execute:
    """
    ["SINTERSTORE","a","a","b"]
    """
  Then the reply equals:
    """
    1
    """
  When I execute:
    """
    ["SISMEMBER","a","b"]
    """
  Then the reply equals:
    """
    1
    """
  When I execute:
    """
    ["SINTERSTORE","a","missing"]
    """
  Then the reply equals:
    """
    0
    """
  When I execute:
    """
    ["EXISTS","a"]
    """
  Then the reply equals:
    """
    0
    """

@command_SUNIONSTORE
Scenario: SUNIONSTORE can overwrite an input after computing the result
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["SADD","a","a","b"],["SADD","b","b","c"]]
    """
  When I execute:
    """
    ["SUNIONSTORE","a","a","b"]
    """
  Then the reply equals:
    """
    3
    """
  When I execute:
    """
    ["SISMEMBER","a","c"]
    """
  Then the reply equals:
    """
    1
    """
  When I execute:
    """
    ["SUNIONSTORE","a","missing"]
    """
  Then the reply equals:
    """
    0
    """
  When I execute:
    """
    ["EXISTS","a"]
    """
  Then the reply equals:
    """
    0
    """

@command_SDIFFCARD
Scenario: SDIFFCARD counts members with LIMIT and rejects invalid bounds
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["SADD","a","a","b"],["SADD","b","b","c"]]
    """
  When I execute:
    """
    ["SDIFFCARD","2","a","b"]
    """
  Then the reply equals:
    """
    1
    """
  When I execute:
    """
    ["SDIFFCARD","2","a","b","LIMIT","1"]
    """
  Then the reply equals:
    """
    1
    """
  When I execute:
    """
    ["SDIFFCARD","2","a","b","LIMIT","0"]
    """
  Then the reply equals:
    """
    1
    """
  When I execute:
    """
    ["SDIFFCARD","1","missing"]
    """
  Then the reply equals:
    """
    0
    """
  When I execute:
    """
    ["SDIFFCARD","0","a"]
    """
  Then the reply is an error with code "ERR"
  When I execute:
    """
    ["SDIFFCARD","1","a","LIMIT","-1"]
    """
  Then the reply is an error with code "ERR"
  When I execute:
    """
    ["SCARD","a"]
    """
  Then the reply equals:
    """
    2
    """

@command_SINTERCARD
Scenario: SINTERCARD counts members with LIMIT and rejects invalid bounds
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["SADD","a","a","b"],["SADD","b","b","c"]]
    """
  When I execute:
    """
    ["SINTERCARD","2","a","b"]
    """
  Then the reply equals:
    """
    1
    """
  When I execute:
    """
    ["SINTERCARD","2","a","b","LIMIT","1"]
    """
  Then the reply equals:
    """
    1
    """
  When I execute:
    """
    ["SINTERCARD","2","a","b","LIMIT","0"]
    """
  Then the reply equals:
    """
    1
    """
  When I execute:
    """
    ["SINTERCARD","1","missing"]
    """
  Then the reply equals:
    """
    0
    """
  When I execute:
    """
    ["SINTERCARD","0","a"]
    """
  Then the reply is an error with code "ERR"
  When I execute:
    """
    ["SINTERCARD","1","a","LIMIT","-1"]
    """
  Then the reply is an error with code "ERR"
  When I execute:
    """
    ["SCARD","a"]
    """
  Then the reply equals:
    """
    2
    """

@command_SUNIONCARD
Scenario: SUNIONCARD counts members with LIMIT and rejects invalid bounds
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["SADD","a","a","b"],["SADD","b","b","c"]]
    """
  When I execute:
    """
    ["SUNIONCARD","2","a","b"]
    """
  Then the reply equals:
    """
    3
    """
  When I execute:
    """
    ["SUNIONCARD","2","a","b","LIMIT","1"]
    """
  Then the reply equals:
    """
    1
    """
  When I execute:
    """
    ["SUNIONCARD","2","a","b","LIMIT","0"]
    """
  Then the reply equals:
    """
    3
    """
  When I execute:
    """
    ["SUNIONCARD","3","a","b","a","APPROX"]
    """
  Then the reply equals:
    """
    3
    """
  When I execute:
    """
    ["SUNIONCARD","2","a","b","APPROX","LIMIT","2"]
    """
  Then the reply equals:
    """
    2
    """
  When I execute:
    """
    ["SUNIONCARD","1","missing"]
    """
  Then the reply equals:
    """
    0
    """
  When I execute:
    """
    ["SUNIONCARD","0","a"]
    """
  Then the reply is an error with code "ERR"
  When I execute:
    """
    ["SUNIONCARD","1","a","LIMIT","-1"]
    """
  Then the reply is an error with code "ERR"
  When I execute:
    """
    ["SCARD","a"]
    """
  Then the reply equals:
    """
    2
    """

@command_SMOVE
Scenario: Moves preserve the source on wrongtype and handle same-key aliases
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["SADD","a","member"],["SET","bad","keep"]]
    """
  When I execute:
    """
    ["SMOVE","a","bad","member"]
    """
  Then the reply is an error with code "WRONGTYPE"
  When I execute:
    """
    ["SISMEMBER","a","member"]
    """
  Then the reply equals:
    """
    1
    """
  When I execute:
    """
    ["SMOVE","a","a","member"]
    """
  Then the reply equals:
    """
    1
    """
  When I execute:
    """
    ["SMOVE","a","b","member"]
    """
  Then the reply equals:
    """
    1
    """
  When I execute:
    """
    ["EXISTS","a"]
    """
  Then the reply equals:
    """
    0
    """
  When I execute:
    """
    ["SISMEMBER","b","member"]
    """
  Then the reply equals:
    """
    1
    """
  When I execute:
    """
    ["SMOVE","missing","b","member"]
    """
  Then the reply equals:
    """
    0
    """

@command_SPOP
Scenario: Popping a singleton deletes the source and rejects negative counts
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["SADD","s","only"]]
    """
  When I execute:
    """
    ["SPOP","s","-1"]
    """
  Then the reply is an error with code "ERR"
  When I execute:
    """
    ["SCARD","s"]
    """
  Then the reply equals:
    """
    1
    """
  When I execute:
    """
    ["SPOP","s"]
    """
  Then the reply equals:
    """
    "only"
    """
  When I execute:
    """
    ["EXISTS","s"]
    """
  Then the reply equals:
    """
    0
    """
  When I execute:
    """
    ["SPOP","s"]
    """
  Then the reply is null

@command_SRANDMEMBER
Scenario: Negative random counts sample with replacement without consuming members
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["SADD","s","only"]]
    """
  When I execute:
    """
    ["SRANDMEMBER","s","-3"]
    """
  Then the reply equals:
    """
    ["only","only","only"]
    """
  When I execute:
    """
    ["SRANDMEMBER","s","0"]
    """
  Then the reply equals:
    """
    []
    """
  When I execute:
    """
    ["SCARD","s"]
    """
  Then the reply equals:
    """
    1
    """
  When I execute:
    """
    ["SRANDMEMBER","missing"]
    """
  Then the reply is null

@command_SSCAN
Scenario: Scan filters a singleton and terminates at cursor zero
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["SADD","s","only"]]
    """
  When I execute:
    """
    ["SSCAN","s","0","MATCH","on*","COUNT","100"]
    """
  Then the reply equals:
    """
    ["0",["only"]]
    """
  When I execute:
    """
    ["SSCAN","s","0","MATCH","no*"]
    """
  Then the reply equals:
    """
    ["0",[]]
    """
  When I execute:
    """
    ["SSCAN","s","invalid"]
    """
  Then the reply is an error with code "ERR"

@command_SADD
Scenario: SADD validates every source without replacing the destination
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["SET","bad","keep"],["SET","dest","keep"]]
    """
  When I execute:
    """
    ["SADD","bad","x"]
    """
  Then the reply is an error with code "WRONGTYPE"
  When I execute:
    """
    ["GET","bad"]
    """
  Then the reply equals:
    """
    "keep"
    """
  When I execute:
    """
    ["GET","dest"]
    """
  Then the reply equals:
    """
    "keep"
    """

@command_SCARD
Scenario: SCARD validates every source without replacing the destination
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["SET","bad","keep"],["SET","dest","keep"]]
    """
  When I execute:
    """
    ["SCARD","bad"]
    """
  Then the reply is an error with code "WRONGTYPE"
  When I execute:
    """
    ["GET","bad"]
    """
  Then the reply equals:
    """
    "keep"
    """
  When I execute:
    """
    ["GET","dest"]
    """
  Then the reply equals:
    """
    "keep"
    """

@command_SDIFF
Scenario: SDIFF validates every source without replacing the destination
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["SET","bad","keep"],["SET","dest","keep"]]
    """
  When I execute:
    """
    ["SDIFF","missing","bad"]
    """
  Then the reply is an error with code "WRONGTYPE"
  When I execute:
    """
    ["GET","bad"]
    """
  Then the reply equals:
    """
    "keep"
    """
  When I execute:
    """
    ["GET","dest"]
    """
  Then the reply equals:
    """
    "keep"
    """

@command_SDIFFCARD
Scenario: SDIFFCARD validates every source without replacing the destination
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["SET","bad","keep"],["SET","dest","keep"]]
    """
  When I execute:
    """
    ["SDIFFCARD","2","missing","bad"]
    """
  Then the reply is an error with code "WRONGTYPE"
  When I execute:
    """
    ["GET","bad"]
    """
  Then the reply equals:
    """
    "keep"
    """
  When I execute:
    """
    ["GET","dest"]
    """
  Then the reply equals:
    """
    "keep"
    """

@command_SDIFFSTORE
Scenario: SDIFFSTORE validates every source without replacing the destination
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["SET","bad","keep"],["SET","dest","keep"]]
    """
  When I execute:
    """
    ["SDIFFSTORE","dest","missing","bad"]
    """
  Then the reply is an error with code "WRONGTYPE"
  When I execute:
    """
    ["GET","bad"]
    """
  Then the reply equals:
    """
    "keep"
    """
  When I execute:
    """
    ["GET","dest"]
    """
  Then the reply equals:
    """
    "keep"
    """

@command_SINTER
Scenario: SINTER validates every source without replacing the destination
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["SET","bad","keep"],["SET","dest","keep"]]
    """
  When I execute:
    """
    ["SINTER","missing","bad"]
    """
  Then the reply is an error with code "WRONGTYPE"
  When I execute:
    """
    ["GET","bad"]
    """
  Then the reply equals:
    """
    "keep"
    """
  When I execute:
    """
    ["GET","dest"]
    """
  Then the reply equals:
    """
    "keep"
    """

@command_SINTERCARD
Scenario: SINTERCARD validates every source without replacing the destination
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["SET","bad","keep"],["SET","dest","keep"]]
    """
  When I execute:
    """
    ["SINTERCARD","2","missing","bad"]
    """
  Then the reply is an error with code "WRONGTYPE"
  When I execute:
    """
    ["GET","bad"]
    """
  Then the reply equals:
    """
    "keep"
    """
  When I execute:
    """
    ["GET","dest"]
    """
  Then the reply equals:
    """
    "keep"
    """

@command_SINTERSTORE
Scenario: SINTERSTORE validates every source without replacing the destination
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["SET","bad","keep"],["SET","dest","keep"]]
    """
  When I execute:
    """
    ["SINTERSTORE","dest","missing","bad"]
    """
  Then the reply is an error with code "WRONGTYPE"
  When I execute:
    """
    ["GET","bad"]
    """
  Then the reply equals:
    """
    "keep"
    """
  When I execute:
    """
    ["GET","dest"]
    """
  Then the reply equals:
    """
    "keep"
    """

@command_SISMEMBER
Scenario: SISMEMBER validates every source without replacing the destination
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["SET","bad","keep"],["SET","dest","keep"]]
    """
  When I execute:
    """
    ["SISMEMBER","bad","x"]
    """
  Then the reply is an error with code "WRONGTYPE"
  When I execute:
    """
    ["GET","bad"]
    """
  Then the reply equals:
    """
    "keep"
    """
  When I execute:
    """
    ["GET","dest"]
    """
  Then the reply equals:
    """
    "keep"
    """

@command_SMEMBERS
Scenario: SMEMBERS validates every source without replacing the destination
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["SET","bad","keep"],["SET","dest","keep"]]
    """
  When I execute:
    """
    ["SMEMBERS","bad"]
    """
  Then the reply is an error with code "WRONGTYPE"
  When I execute:
    """
    ["GET","bad"]
    """
  Then the reply equals:
    """
    "keep"
    """
  When I execute:
    """
    ["GET","dest"]
    """
  Then the reply equals:
    """
    "keep"
    """

@command_SMISMEMBER
Scenario: SMISMEMBER validates every source without replacing the destination
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["SET","bad","keep"],["SET","dest","keep"]]
    """
  When I execute:
    """
    ["SMISMEMBER","bad","x"]
    """
  Then the reply is an error with code "WRONGTYPE"
  When I execute:
    """
    ["GET","bad"]
    """
  Then the reply equals:
    """
    "keep"
    """
  When I execute:
    """
    ["GET","dest"]
    """
  Then the reply equals:
    """
    "keep"
    """

@command_SPOP
Scenario: SPOP validates every source without replacing the destination
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["SET","bad","keep"],["SET","dest","keep"]]
    """
  When I execute:
    """
    ["SPOP","bad"]
    """
  Then the reply is an error with code "WRONGTYPE"
  When I execute:
    """
    ["GET","bad"]
    """
  Then the reply equals:
    """
    "keep"
    """
  When I execute:
    """
    ["GET","dest"]
    """
  Then the reply equals:
    """
    "keep"
    """

@command_SRANDMEMBER
Scenario: SRANDMEMBER validates every source without replacing the destination
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["SET","bad","keep"],["SET","dest","keep"]]
    """
  When I execute:
    """
    ["SRANDMEMBER","bad"]
    """
  Then the reply is an error with code "WRONGTYPE"
  When I execute:
    """
    ["GET","bad"]
    """
  Then the reply equals:
    """
    "keep"
    """
  When I execute:
    """
    ["GET","dest"]
    """
  Then the reply equals:
    """
    "keep"
    """

@command_SREM
Scenario: SREM validates every source without replacing the destination
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["SET","bad","keep"],["SET","dest","keep"]]
    """
  When I execute:
    """
    ["SREM","bad","x"]
    """
  Then the reply is an error with code "WRONGTYPE"
  When I execute:
    """
    ["GET","bad"]
    """
  Then the reply equals:
    """
    "keep"
    """
  When I execute:
    """
    ["GET","dest"]
    """
  Then the reply equals:
    """
    "keep"
    """

@command_SSCAN
Scenario: SSCAN validates every source without replacing the destination
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["SET","bad","keep"],["SET","dest","keep"]]
    """
  When I execute:
    """
    ["SSCAN","bad","0"]
    """
  Then the reply is an error with code "WRONGTYPE"
  When I execute:
    """
    ["GET","bad"]
    """
  Then the reply equals:
    """
    "keep"
    """
  When I execute:
    """
    ["GET","dest"]
    """
  Then the reply equals:
    """
    "keep"
    """

@command_SUNION
Scenario: SUNION validates every source without replacing the destination
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["SET","bad","keep"],["SET","dest","keep"]]
    """
  When I execute:
    """
    ["SUNION","missing","bad"]
    """
  Then the reply is an error with code "WRONGTYPE"
  When I execute:
    """
    ["GET","bad"]
    """
  Then the reply equals:
    """
    "keep"
    """
  When I execute:
    """
    ["GET","dest"]
    """
  Then the reply equals:
    """
    "keep"
    """

@command_SUNIONCARD
Scenario: SUNIONCARD validates every source without replacing the destination
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["SET","bad","keep"],["SET","dest","keep"]]
    """
  When I execute:
    """
    ["SUNIONCARD","2","missing","bad","LIMIT","1"]
    """
  Then the reply is an error with code "WRONGTYPE"
  When I execute:
    """
    ["GET","bad"]
    """
  Then the reply equals:
    """
    "keep"
    """
  When I execute:
    """
    ["GET","dest"]
    """
  Then the reply equals:
    """
    "keep"
    """

@command_SUNIONSTORE
Scenario: SUNIONSTORE validates every source without replacing the destination
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["SET","bad","keep"],["SET","dest","keep"]]
    """
  When I execute:
    """
    ["SUNIONSTORE","dest","missing","bad"]
    """
  Then the reply is an error with code "WRONGTYPE"
  When I execute:
    """
    ["GET","bad"]
    """
  Then the reply equals:
    """
    "keep"
    """
  When I execute:
    """
    ["GET","dest"]
    """
  Then the reply equals:
    """
    "keep"
    """

@command_SDIFFCARD @command_SINTERCARD @command_SUNIONCARD
Scenario: Counted aliases and option words used as real keys remain unambiguous
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["SADD","LIMIT","a","b","c"],["SADD","APPROX","b","c","d"]]
    """
  When I execute:
    """
    ["SDIFFCARD","2","LIMIT","LIMIT"]
    """
  Then the reply equals:
    """
    0
    """
  When I execute:
    """
    ["SINTERCARD","2","LIMIT","LIMIT"]
    """
  Then the reply equals:
    """
    3
    """
  When I execute:
    """
    ["SUNIONCARD","2","LIMIT","APPROX","APPROX","LIMIT","2"]
    """
  Then the reply equals:
    """
    2
    """
  When I execute:
    """
    ["SUNIONCARD","2","LIMIT","APPROX"]
    """
  Then the reply equals:
    """
    4
    """
  When I execute:
    """
    ["SCARD","LIMIT"]
    """
  Then the reply equals:
    """
    3
    """

