Feature: Redis hash commands and field expiration

@command_HSET @command_HGET @command_HMGET @command_HGETALL
Scenario: Hash writes count new binary-safe fields and preserve requested read order
  Given a fresh ffkv server
  And client "map" uses RESP 3
  When I execute:
    """
    ["HSET","h","a","1","b","2"]
    """
  Then the reply equals:
    """
    2
    """
  When I execute:
    """
    ["HSET","h","a","3"]
    """
  Then the reply equals:
    """
    0
    """
  When I execute:
    """
    ["HGET","h","a"]
    """
  Then the reply equals:
    """
    "3"
    """
  When I execute:
    """
    ["HMGET","h","b","missing","a"]
    """
  Then the reply equals:
    """
    ["2",null,"3"]
    """
  When client "map" executes:
    """
    ["HGETALL","h"]
    """
  Then the reply equals:
    """
    {"a":"3","b":"2"}
    """

@command_HSETNX @command_HEXISTS
Scenario: Conditional creation cannot overwrite an existing field
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["HSET","h","a","old"]]
    """
  When I execute:
    """
    ["HSETNX","h","a","new"]
    """
  Then the reply equals:
    """
    0
    """
  When I execute:
    """
    ["HSETNX","h","b","created"]
    """
  Then the reply equals:
    """
    1
    """
  When I execute:
    """
    ["HEXISTS","h","b"]
    """
  Then the reply equals:
    """
    1
    """
  When I execute:
    """
    ["HEXISTS","h","missing"]
    """
  Then the reply equals:
    """
    0
    """
  When I execute:
    """
    ["HGET","h","a"]
    """
  Then the reply equals:
    """
    "old"
    """

@command_HMSET @command_HLEN
Scenario: Legacy multiple writes retain last duplicate value
  Given a fresh ffkv server
  When I execute:
    """
    ["HMSET","h","a","first","a","last"]
    """
  Then the reply equals:
    """
    "OK"
    """
  When I execute:
    """
    ["HLEN","h"]
    """
  Then the reply equals:
    """
    1
    """
  When I execute:
    """
    ["HGET","h","a"]
    """
  Then the reply equals:
    """
    "last"
    """

@command_HKEYS @command_HVALS @command_HSTRLEN
Scenario: Hash field enumeration and byte lengths differ from character counts
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["HSET","h","\u00e9","\ud83d\ude42"]]
    """
  When I execute:
    """
    ["HKEYS","h"]
    """
  Then the reply equals:
    """
    ["\u00e9"]
    """
  When I execute:
    """
    ["HVALS","h"]
    """
  Then the reply equals:
    """
    ["\ud83d\ude42"]
    """
  When I execute:
    """
    ["HSTRLEN","h","\u00e9"]
    """
  Then the reply equals:
    """
    4
    """
  When I execute:
    """
    ["HSTRLEN","h","absent"]
    """
  Then the reply equals:
    """
    0
    """

@command_HDEL
Scenario: Deletion counts each existing field only once and removes an empty key
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["HSET","h","a","1"]]
    """
  When I execute:
    """
    ["HDEL","h","a","a","missing"]
    """
  Then the reply equals:
    """
    1
    """
  When I execute:
    """
    ["EXISTS","h"]
    """
  Then the reply equals:
    """
    0
    """
  When I execute:
    """
    ["HDEL","h","a"]
    """
  Then the reply equals:
    """
    0
    """

@command_HINCRBY
Scenario: Signed integer increments preserve existing field deadlines
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["HSET","h","n","10"],["HPEXPIREAT","h","4102444800000","FIELDS","1","n"]]
    """
  When I execute:
    """
    ["HINCRBY","h","n","-3"]
    """
  Then the reply equals:
    """
    7
    """
  When I execute:
    """
    ["HPEXPIRETIME","h","FIELDS","1","n"]
    """
  Then the reply equals:
    """
    [4102444800000]
    """
  When I execute:
    """
    ["HSET","h","n","9223372036854775807"]
    """
  Then the reply equals:
    """
    0
    """
  When I execute:
    """
    ["HINCRBY","h","n","1"]
    """
  Then the reply is an error with code "ERR"
  When I execute:
    """
    ["HGET","h","n"]
    """
  Then the reply equals:
    """
    "9223372036854775807"
    """

@command_HINCRBYFLOAT
Scenario: Floating increments normalize decimals and resolve durable values
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["HSET","h","n","10.50"],["HPEXPIREAT","h","4102444800000","FIELDS","1","n"]]
    """
  When I execute:
    """
    ["HINCRBYFLOAT","h","n","0.1"]
    """
  Then the reply equals:
    """
    "10.6"
    """
  When I execute:
    """
    ["HINCRBYFLOAT","h","n","-5"]
    """
  Then the reply equals:
    """
    "5.6"
    """
  When I execute:
    """
    ["HPEXPIRETIME","h","FIELDS","1","n"]
    """
  Then the reply equals:
    """
    [4102444800000]
    """
  When I execute:
    """
    ["HINCRBYFLOAT","h","n","nan"]
    """
  Then the reply is an error with code "ERR"
  When I execute:
    """
    ["HGET","h","n"]
    """
  Then the reply equals:
    """
    "5.6"
    """

@command_HSCAN
Scenario: Scanning supports matching and names-only replies without changing the hash
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["HSET","h","account:1","Alice","other","Bob"]]
    """
  When I execute:
    """
    ["HSCAN","h","0","MATCH","account:*","COUNT","100","NOVALUES"]
    """
  Then the reply equals:
    """
    ["0",["account:1"]]
    """
  When I execute:
    """
    ["HSCAN","h","0","MATCH","account:*","COUNT","100"]
    """
  Then the reply equals:
    """
    ["0",["account:1","Alice"]]
    """
  When I execute:
    """
    ["HLEN","h"]
    """
  Then the reply equals:
    """
    2
    """

@command_HGETDEL
Scenario: Get-and-delete processes duplicate fields in order and deletes the last-field key
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["HSET","h","a","one","b","two"]]
    """
  When I execute:
    """
    ["HGETDEL","h","FIELDS","4","a","a","missing","b"]
    """
  Then the reply equals:
    """
    ["one",null,null,"two"]
    """
  When I execute:
    """
    ["EXISTS","h"]
    """
  Then the reply equals:
    """
    0
    """

@command_HGETEX
Scenario: Get-with-expiry supports absolute deadlines persistence and immediate expiration
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["HSET","h","a","one","b","two"]]
    """
  When I execute:
    """
    ["HGETEX","h","PXAT","4102444800000","FIELDS","2","a","missing"]
    """
  Then the reply equals:
    """
    ["one",null]
    """
  When I execute:
    """
    ["HPEXPIRETIME","h","FIELDS","1","a"]
    """
  Then the reply equals:
    """
    [4102444800000]
    """
  When I execute:
    """
    ["HGETEX","h","PERSIST","FIELDS","1","a"]
    """
  Then the reply equals:
    """
    ["one"]
    """
  When I execute:
    """
    ["HPTTL","h","FIELDS","1","a"]
    """
  Then the reply equals:
    """
    [-1]
    """
  When I execute:
    """
    ["HGETEX","h","PXAT","1","FIELDS","2","a","a"]
    """
  Then the reply equals:
    """
    ["one",null]
    """
  When I execute:
    """
    ["HGET","h","a"]
    """
  Then the reply is null
  When I execute:
    """
    ["HGET","h","b"]
    """
  Then the reply equals:
    """
    "two"
    """

@command_HGETEX @command_HGETDEL
Scenario: Malformed field blocks never consume values
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["HSET","h","a","one"]]
    """
  When I execute:
    """
    ["HGETDEL","h","FIELDS","2","a"]
    """
  Then the reply is an error with code "ERR"
  When I execute:
    """
    ["HGETEX","h","PERSIST","PX","10","FIELDS","1","a"]
    """
  Then the reply is an error with code "ERR"
  When I execute:
    """
    ["HGET","h","a"]
    """
  Then the reply equals:
    """
    "one"
    """

@command_HSETEX
Scenario: All-fields conditions are atomic and keep existing field expirations
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["HSET","h","a","one"],["HPEXPIREAT","h","4102444800000","FIELDS","1","a"]]
    """
  When I execute:
    """
    ["HSETEX","h","FNX","FIELDS","2","a","changed","b","new"]
    """
  Then the reply equals:
    """
    0
    """
  When I execute:
    """
    ["HMGET","h","a","b"]
    """
  Then the reply equals:
    """
    ["one",null]
    """
  When I execute:
    """
    ["HSETEX","h","FXX","KEEPTTL","FIELDS","1","a","updated"]
    """
  Then the reply equals:
    """
    1
    """
  When I execute:
    """
    ["HPEXPIRETIME","h","FIELDS","1","a"]
    """
  Then the reply equals:
    """
    [4102444800000]
    """
  When I execute:
    """
    ["HSETEX","h","PXAT","1","FIELDS","1","a","gone"]
    """
  Then the reply equals:
    """
    1
    """
  When I execute:
    """
    ["EXISTS","h"]
    """
  Then the reply equals:
    """
    0
    """

@command_HEXPIRE
Scenario: HEXPIRE reports per-field outcomes and respects expiration conditions
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["HSET","h","a","one","b","two"]]
    """
  When I execute:
    """
    ["HEXPIRE","h","3600","NX","FIELDS","3","a","b","missing"]
    """
  Then the reply equals:
    """
    [1,1,-2]
    """
  When I execute:
    """
    ["HEXPIRE","h","1","NX","FIELDS","1","a"]
    """
  Then the reply equals:
    """
    [0]
    """
  When I execute:
    """
    ["HEXPIRE","h","0","XX","FIELDS","2","a","b"]
    """
  Then the reply equals:
    """
    [2,2]
    """
  When I execute:
    """
    ["EXISTS","h"]
    """
  Then the reply equals:
    """
    0
    """

@command_HPEXPIRE
Scenario: HPEXPIRE reports per-field outcomes and respects expiration conditions
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["HSET","h","a","one","b","two"]]
    """
  When I execute:
    """
    ["HPEXPIRE","h","3600000","NX","FIELDS","3","a","b","missing"]
    """
  Then the reply equals:
    """
    [1,1,-2]
    """
  When I execute:
    """
    ["HPEXPIRE","h","1","NX","FIELDS","1","a"]
    """
  Then the reply equals:
    """
    [0]
    """
  When I execute:
    """
    ["HPEXPIRE","h","0","XX","FIELDS","2","a","b"]
    """
  Then the reply equals:
    """
    [2,2]
    """
  When I execute:
    """
    ["EXISTS","h"]
    """
  Then the reply equals:
    """
    0
    """

@command_HEXPIREAT @command_HEXPIRETIME
Scenario: HEXPIREAT exposes absolute deadlines and independent conditional outcomes
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["HSET","h","a","one","b","two"]]
    """
  When I execute:
    """
    ["HEXPIREAT","h","4102444800","FIELDS","2","a","missing"]
    """
  Then the reply equals:
    """
    [1,-2]
    """
  When I execute:
    """
    ["HEXPIRETIME","h","FIELDS","3","a","b","missing"]
    """
  Then the reply equals:
    """
    [4102444800,-1,-2]
    """
  When I execute:
    """
    ["HEXPIREAT","h","4102444700","GT","FIELDS","1","a"]
    """
  Then the reply equals:
    """
    [0]
    """
  When I execute:
    """
    ["HEXPIREAT","h","1","LT","FIELDS","1","a"]
    """
  Then the reply equals:
    """
    [2]
    """
  When I execute:
    """
    ["HGET","h","b"]
    """
  Then the reply equals:
    """
    "two"
    """

@command_HPEXPIREAT @command_HPEXPIRETIME
Scenario: HPEXPIREAT exposes absolute deadlines and independent conditional outcomes
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["HSET","h","a","one","b","two"]]
    """
  When I execute:
    """
    ["HPEXPIREAT","h","4102444800000","FIELDS","2","a","missing"]
    """
  Then the reply equals:
    """
    [1,-2]
    """
  When I execute:
    """
    ["HPEXPIRETIME","h","FIELDS","3","a","b","missing"]
    """
  Then the reply equals:
    """
    [4102444800000,-1,-2]
    """
  When I execute:
    """
    ["HPEXPIREAT","h","4102444799900","GT","FIELDS","1","a"]
    """
  Then the reply equals:
    """
    [0]
    """
  When I execute:
    """
    ["HPEXPIREAT","h","1","LT","FIELDS","1","a"]
    """
  Then the reply equals:
    """
    [2]
    """
  When I execute:
    """
    ["HGET","h","b"]
    """
  Then the reply equals:
    """
    "two"
    """

@command_HTTL @command_HPTTL @command_HPERSIST
Scenario: TTL states distinguish persistent missing and removed expirations
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["HSET","h","a","one","b","two"],["HPEXPIREAT","h","4102444800000","FIELDS","1","a"]]
    """
  When I execute:
    """
    ["HPERSIST","h","FIELDS","3","a","b","missing"]
    """
  Then the reply equals:
    """
    [1,-1,-2]
    """
  When I execute:
    """
    ["HTTL","h","FIELDS","3","a","b","missing"]
    """
  Then the reply equals:
    """
    [-1,-1,-2]
    """
  When I execute:
    """
    ["HPTTL","h","FIELDS","3","a","b","missing"]
    """
  Then the reply equals:
    """
    [-1,-1,-2]
    """
  When I execute:
    """
    ["HPERSIST","h","FIELDS","1","a"]
    """
  Then the reply equals:
    """
    [-1]
    """

@command_HRANDFIELD
Scenario: Random field count modes and RESP2 values preserve duplicates correctly
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["HSET","h","only","value"]]
    """
  When I execute:
    """
    ["HRANDFIELD","h"]
    """
  Then the reply equals:
    """
    "only"
    """
  When I execute:
    """
    ["HRANDFIELD","h","10"]
    """
  Then the reply equals:
    """
    ["only"]
    """
  When I execute:
    """
    ["HRANDFIELD","h","-3","WITHVALUES"]
    """
  Then the reply equals:
    """
    ["only","value","only","value","only","value"]
    """
  When I execute:
    """
    ["HRANDFIELD","h","0","WITHVALUES"]
    """
  Then the reply equals:
    """
    []
    """
  When I execute:
    """
    ["HRANDFIELD","absent"]
    """
  Then the reply is null
  When I execute:
    """
    ["HRANDFIELD","absent","-2"]
    """
  Then the reply equals:
    """
    []
    """
  When I execute:
    """
    ["HRANDFIELD","h","1","bad"]
    """
  Then the reply is an error with code "ERR"
  When I execute:
    """
    ["HLEN","h"]
    """
  Then the reply equals:
    """
    1
    """

@command_HRANDFIELD
Scenario: RESP3 random values are ordered field-value pairs
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["HSET","h","only","value"]]
    """
  Given client "three" uses RESP 3
  When client "three" executes:
    """
    ["HRANDFIELD","h","-2","WITHVALUES"]
    """
  Then the reply equals:
    """
    [["only","value"],["only","value"]]
    """

@command_HSET @command_HINCRBY @command_HINCRBYFLOAT
Scenario: Malformed writes and numeric grammar errors leave the hash unchanged
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["HSET","h","n","01"]]
    """
  When I execute:
    """
    ["HSET","h","a","one","dangling"]
    """
  Then the reply is an error with code "ERR"
  When I execute:
    """
    ["HINCRBY","h","n","1"]
    """
  Then the reply is an error with code "ERR"
  When I execute:
    """
    ["HINCRBYFLOAT","h","n"," 1"]
    """
  Then the reply is an error with code "ERR"
  When I execute:
    """
    ["HMGET","h","n","a"]
    """
  Then the reply equals:
    """
    ["01",null]
    """

@command_HGET @command_HGETDEL @command_HGETEX @command_HRANDFIELD
Scenario: Hash operations reject incompatible key types
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["SET","h","string"]]
    """
  When I execute:
    """
    ["HGET","h","a"]
    """
  Then the reply is an error with code "WRONGTYPE"
  When I execute:
    """
    ["HGETDEL","h","FIELDS","1","a"]
    """
  Then the reply is an error with code "WRONGTYPE"
  When I execute:
    """
    ["HGETEX","h","FIELDS","1","a"]
    """
  Then the reply is an error with code "WRONGTYPE"
  When I execute:
    """
    ["HRANDFIELD","h","1"]
    """
  Then the reply is an error with code "WRONGTYPE"
  When I execute:
    """
    ["GET","h"]
    """
  Then the reply equals:
    """
    "string"
    """

@command_HGETDEL @command_HGETEX @command_HINCRBYFLOAT
Scenario: Hash mutations and field deadlines survive crash recovery
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["HSET","h","a","1.5","b","two"],["HGETEX","h","PXAT","4102444800000","FIELDS","1","a"],["HINCRBYFLOAT","h","a","0.5"],["HGETDEL","h","FIELDS","1","b"]]
    """
  When ffkv restarts after a crash
  When I execute:
    """
    ["HMGET","h","a","b"]
    """
  Then the reply equals:
    """
    ["2",null]
    """
  When I execute:
    """
    ["HPEXPIRETIME","h","FIELDS","1","a"]
    """
  Then the reply equals:
    """
    [4102444800000]
    """

@command_HIMPORT @command_HIMPORT_PREPARE @command_HIMPORT_SET
Scenario: Prepared layouts preserve declared field order and replace complete hashes
  Given a fresh ffkv server
  And client "map" uses RESP 3
  And the following commands succeed:
    """
    [["HSET","user","old","discarded"],["PEXPIREAT","user","4102444800000"],["HPEXPIREAT","user","4102444800000","FIELDS","1","old"],["HIMPORT","PREPARE","layout","name","email","age"]]
    """
  When I execute:
    """
    ["HIMPORT","SET","user","layout","Alice","a@example.com","30"]
    """
  Then the reply equals:
    """
    "OK"
    """
  When client "map" executes:
    """
    ["HGETALL","user"]
    """
  Then the reply equals:
    """
    {"name":"Alice","email":"a@example.com","age":"30"}
    """
  When I execute:
    """
    ["PTTL","user"]
    """
  Then the reply equals:
    """
    -1
    """
  When I execute:
    """
    ["HPTTL","user","FIELDS","3","name","email","age"]
    """
  Then the reply equals:
    """
    [-1,-1,-1]
    """
  When I execute:
    """
    ["HIMPORT","SET","other","layout","Bob","b@example.com","25"]
    """
  Then the reply equals:
    """
    "OK"
    """
  When I execute:
    """
    ["HGET","other","name"]
    """
  Then the reply equals:
    """
    "Bob"
    """

@command_HIMPORT_PREPARE @command_HIMPORT_SET
Scenario: Duplicate layout preparation and mismatched imports preserve old state
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["HIMPORT","PREPARE","layout","b","a"],["HIMPORT","SET","h","layout","bee","aye"],["HPEXPIREAT","h","4102444800000","FIELDS","1","b"]]
    """
  When I execute:
    """
    ["HIMPORT","PREPARE","layout","duplicate","duplicate"]
    """
  Then the reply is an error with code "ERR"
  When I execute:
    """
    ["HIMPORT","SET","h","layout","only-one-value"]
    """
  Then the reply is an error with code "ERR"
  When I execute:
    """
    ["HMGET","h","b","a"]
    """
  Then the reply equals:
    """
    ["bee","aye"]
    """
  When I execute:
    """
    ["HPEXPIRETIME","h","FIELDS","1","b"]
    """
  Then the reply equals:
    """
    [4102444800000]
    """
  When I execute:
    """
    ["HIMPORT","SET","new","layout","second-bee","second-aye"]
    """
  Then the reply equals:
    """
    "OK"
    """
  When I execute:
    """
    ["HMGET","new","b","a"]
    """
  Then the reply equals:
    """
    ["second-bee","second-aye"]
    """

@command_HIMPORT_DISCARD @command_HIMPORT_DISCARDALL
Scenario: Discarding session definitions never removes imported hashes
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["HIMPORT","PREPARE","one","a"],["HIMPORT","PREPARE","two","b"],["HIMPORT","SET","h","one","value"]]
    """
  When I execute:
    """
    ["HIMPORT","DISCARD","one"]
    """
  Then the reply equals:
    """
    1
    """
  When I execute:
    """
    ["HIMPORT","DISCARD","one"]
    """
  Then the reply equals:
    """
    0
    """
  When I execute:
    """
    ["HIMPORT","SET","missing","one","no"]
    """
  Then the reply is an error with code "ERR"
  When I execute:
    """
    ["HIMPORT","DISCARDALL"]
    """
  Then the reply equals:
    """
    1
    """
  When I execute:
    """
    ["HIMPORT","DISCARDALL"]
    """
  Then the reply equals:
    """
    0
    """
  When I execute:
    """
    ["HGET","h","a"]
    """
  Then the reply equals:
    """
    "value"
    """
  When I execute:
    """
    ["HIMPORT","SET","missing","two","no"]
    """
  Then the reply is an error with code "ERR"

@command_HIMPORT_SET
Scenario: Non-hash target type takes precedence over missing definitions
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["SET","wrong","original"]]
    """
  When I execute:
    """
    ["HIMPORT","SET","wrong","undefined","value"]
    """
  Then the reply is an error with code "WRONGTYPE"
  When I execute:
    """
    ["GET","wrong"]
    """
  Then the reply equals:
    """
    "original"
    """
  When I execute:
    """
    ["HIMPORT","SET","absent","undefined","value"]
    """
  Then the reply is an error with code "ERR"
  When I execute:
    """
    ["EXISTS","absent"]
    """
  Then the reply equals:
    """
    0
    """

@command_HIMPORT_PREPARE @command_HIMPORT_SET
Scenario: Replacing layout definitions affects only future imports
  Given a fresh ffkv server
  And client "map" uses RESP 3
  And the following commands succeed:
    """
    [["HIMPORT","PREPARE","layout","old"],["HIMPORT","SET","before","layout","value"]]
    """
  When I execute:
    """
    ["HIMPORT","PREPARE","layout","new"]
    """
  Then the reply equals:
    """
    "OK"
    """
  When I execute:
    """
    ["HIMPORT","SET","after","layout","updated"]
    """
  Then the reply equals:
    """
    "OK"
    """
  When client "map" executes:
    """
    ["HGETALL","before"]
    """
  Then the reply equals:
    """
    {"old":"value"}
    """
  When client "map" executes:
    """
    ["HGETALL","after"]
    """
  Then the reply equals:
    """
    {"new":"updated"}
    """

@command_HIMPORT_PREPARE @command_HIMPORT_SET
Scenario: Import layouts are isolated per connection
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["HIMPORT","PREPARE","layout","a"]]
    """
  When client "other" executes:
    """
    ["HIMPORT","SET","other-key","layout","value"]
    """
  Then the reply is an error with code "ERR"
  When I execute:
    """
    ["HIMPORT","SET","h","layout","value"]
    """
  Then the reply equals:
    """
    "OK"
    """
  When client "other" executes:
    """
    ["HGET","h","a"]
    """
  Then the reply equals:
    """
    "value"
    """

@command_HIMPORT_PREPARE @command_HIMPORT_SET
Scenario: Reset forgets definitions without changing imported hashes
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["HIMPORT","PREPARE","layout","a"],["HIMPORT","SET","h","layout","value"]]
    """
  When I execute:
    """
    ["RESET"]
    """
  Then the reply equals:
    """
    "RESET"
    """
  When I execute:
    """
    ["HIMPORT","SET","new","layout","value"]
    """
  Then the reply is an error with code "ERR"
  When I execute:
    """
    ["HGET","h","a"]
    """
  Then the reply equals:
    """
    "value"
    """

@command_HIMPORT_SET
Scenario: Imported values survive recovery without resurrecting session definitions
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["HIMPORT","PREPARE","layout","a"],["HIMPORT","SET","h","layout","value"]]
    """
  When ffkv restarts after a crash
  When I execute:
    """
    ["HGET","h","a"]
    """
  Then the reply equals:
    """
    "value"
    """
  When I execute:
    """
    ["HIMPORT","SET","new","layout","value"]
    """
  Then the reply is an error with code "ERR"

@command_HIMPORT_PREPARE @command_HIMPORT_SET
Scenario: Transactions apply session imports in execution order
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["MULTI"],["HIMPORT","PREPARE","layout","a"],["HIMPORT","SET","h","layout","value"]]
    """
  When I execute:
    """
    ["EXEC"]
    """
  Then the reply equals:
    """
    ["OK","OK"]
    """
  When I execute:
    """
    ["HGET","h","a"]
    """
  Then the reply equals:
    """
    "value"
    """
  When I execute:
    """
    ["HIMPORT","SET","next","layout","second"]
    """
  Then the reply equals:
    """
    "OK"
    """

@command_HIMPORT_DISCARD
Scenario: Discarded transactions never discard prepared definitions
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["HIMPORT","PREPARE","layout","a"],["MULTI"],["HIMPORT","DISCARD","layout"],["DISCARD"]]
    """
  When I execute:
    """
    ["HIMPORT","SET","h","layout","value"]
    """
  Then the reply equals:
    """
    "OK"
    """
  When I execute:
    """
    ["HGET","h","a"]
    """
  Then the reply equals:
    """
    "value"
    """

@command_HIMPORT_PREPARE @command_HIMPORT_SET
Scenario: Scripts use invocation-local layouts rather than the network session
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["HIMPORT","PREPARE","network","a"]]
    """
  When I execute:
    """
    ["EVAL","return redis.call('HIMPORT','SET',KEYS[1],'network','value')","1","h"]
    """
  Then the reply is an error with code "ERR"
  When I execute:
    """
    ["EVAL","redis.call('HIMPORT','PREPARE','inside','a'); redis.call('HIMPORT','SET',KEYS[1],'inside','value'); return redis.call('HGET',KEYS[1],'a')","1","h"]
    """
  Then the reply equals:
    """
    "value"
    """
  When I execute:
    """
    ["EVAL","return redis.call('HIMPORT','SET',KEYS[1],'inside','next')","1","h"]
    """
  Then the reply is an error with code "ERR"
  When I execute:
    """
    ["HIMPORT","SET","network-key","network","still-present"]
    """
  Then the reply equals:
    """
    "OK"
    """

@command_HIMPORT_PREPARE @command_HIMPORT_SET
Scenario: Import names fields and values are binary safe
  Given a fresh ffkv server
  When I execute:
    """
    ["HIMPORT","PREPARE",{"base64":"AP8="},{"base64":"/wA="},""]
    """
  Then the reply equals:
    """
    "OK"
    """
  When I execute:
    """
    ["HIMPORT","SET","h",{"base64":"AP8="},{"base64":"APA="},"empty-field"]
    """
  Then the reply equals:
    """
    "OK"
    """
  When I execute:
    """
    ["HGET","h",{"base64":"/wA="}]
    """
  Then the reply equals:
    """
    {"type":"bytes","base64":"APA="}
    """
  When I execute:
    """
    ["HGET","h",""]
    """
  Then the reply equals:
    """
    "empty-field"
    """

@command_HGETEX @command_HSETEX
Scenario: Counted field blocks permit trailing options and keyword field names
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["HSET","h","EX","old"]]
    """
  When I execute:
    """
    ["HGETEX","h","FIELDS","1","EX","PXAT","4102444800000"]
    """
  Then the reply equals:
    """
    ["old"]
    """
  When I execute:
    """
    ["HPEXPIRETIME","h","FIELDS","1","EX"]
    """
  Then the reply equals:
    """
    [4102444800000]
    """
  When I execute:
    """
    ["HSETEX","h","FIELDS","1","EX","new","FXX","KEEPTTL"]
    """
  Then the reply equals:
    """
    1
    """
  When I execute:
    """
    ["HPEXPIRETIME","h","FIELDS","1","EX"]
    """
  Then the reply equals:
    """
    [4102444800000]
    """
  When I execute:
    """
    ["HGETEX","h","PERSIST","FIELDS","1","EX","PERSIST"]
    """
  Then the reply is an error with code "ERR"
  When I execute:
    """
    ["HGET","h","EX"]
    """
  Then the reply equals:
    """
    "new"
    """

@command_HGETDEL @command_HGETEX
Scenario: Existing type errors precede malformed field and expiry options
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["SET","h","original"]]
    """
  When I execute:
    """
    ["HGETDEL","h","BAD","2","field"]
    """
  Then the reply is an error with code "WRONGTYPE"
  When I execute:
    """
    ["HGETEX","h","PX","invalid","FIELDS","1","field"]
    """
  Then the reply is an error with code "WRONGTYPE"
  When I execute:
    """
    ["GET","h"]
    """
  Then the reply equals:
    """
    "original"
    """

@command_HINCRBYFLOAT
Scenario: Extended-precision float arithmetic avoids double rounding
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["HSET","h","fraction","0.1","large","9007199254740992","scientific","5.0e3","zero","-0"]]
    """
  When I execute:
    """
    ["HINCRBYFLOAT","h","fraction","0.2"]
    """
  Then the reply equals:
    """
    "0.3"
    """
  When I execute:
    """
    ["HINCRBYFLOAT","h","large","1"]
    """
  Then the reply equals:
    """
    "9007199254740993"
    """
  When I execute:
    """
    ["HINCRBYFLOAT","h","scientific","2.0e2"]
    """
  Then the reply equals:
    """
    "5200"
    """
  When I execute:
    """
    ["HINCRBYFLOAT","h","zero","0"]
    """
  Then the reply equals:
    """
    "0"
    """
  When I execute:
    """
    ["HINCRBYFLOAT","h","large","1e5000"]
    """
  Then the reply is an error with code "ERR"
  When I execute:
    """
    ["HGET","h","large"]
    """
  Then the reply equals:
    """
    "9007199254740993"
    """

@command_HGETEX
Scenario: Zero relative expiry is immediately destructive and duplicate-safe
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["HSET","h","a","value"]]
    """
  When I execute:
    """
    ["HGETEX","h","FIELDS","2","a","a","PX","0"]
    """
  Then the reply equals:
    """
    ["value",null]
    """
  When I execute:
    """
    ["EXISTS","h"]
    """
  Then the reply equals:
    """
    0
    """
  When I execute:
    """
    ["HGETEX","absent","FIELDS","1","a","EX","-1"]
    """
  Then the reply is an error with code "ERR"
  When I execute:
    """
    ["EXISTS","absent"]
    """
  Then the reply equals:
    """
    0
    """

@command_HEXPIRE @command_HPEXPIRE @command_HEXPIREAT @command_HPEXPIREAT
Scenario: Expiry conditions may follow counted fields and mixed conditions fail atomically
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["HSET","h","a","one","b","two"]]
    """
  When I execute:
    """
    ["HEXPIRE","h","3600","FIELDS","2","a","b","NX"]
    """
  Then the reply equals:
    """
    [1,1]
    """
  When I execute:
    """
    ["HPEXPIRE","h","0","FIELDS","1","a","XX","XX"]
    """
  Then the reply equals:
    """
    [2]
    """
  When I execute:
    """
    ["HEXPIREAT","h","4102444800","FIELDS","1","b","GT"]
    """
  Then the reply equals:
    """
    [1]
    """
  When I execute:
    """
    ["HPEXPIREAT","h","1","FIELDS","1","b","NX","LT"]
    """
  Then the reply is an error with code "ERR"
  When I execute:
    """
    ["HGET","h","b"]
    """
  Then the reply equals:
    """
    "two"
    """
  When I execute:
    """
    ["HPEXPIRETIME","h","FIELDS","1","b"]
    """
  Then the reply equals:
    """
    [4102444800000]
    """

@command_HRANDFIELD
Scenario: WITHVALUES count overflow is rejected before looking up the key
  Given a fresh ffkv server
  When I execute:
    """
    ["HRANDFIELD","absent","9223372036854775807","WITHVALUES"]
    """
  Then the reply is an error with code "ERR"
  When I execute:
    """
    ["HRANDFIELD","absent","-9223372036854775808"]
    """
  Then the reply is an error with code "ERR"
  When I execute:
    """
    ["HRANDFIELD","absent","01"]
    """
  Then the reply is an error with code "ERR"

@command_HRANDFIELD
Scenario: Random access indexes survive field deletion expiry and reinsertion
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["HSET","h","a","one","b","two","c","three"],["HDEL","h","b"],["HPEXPIRE","h","0","FIELDS","1","c"]]
    """
  When I execute:
    """
    ["HRANDFIELD","h"]
    """
  Then the reply equals:
    """
    "a"
    """
  When I execute:
    """
    ["HSET","h","b","second"]
    """
  Then the reply equals:
    """
    1
    """
  When I execute:
    """
    ["HDEL","h","a"]
    """
  Then the reply equals:
    """
    1
    """
  When I execute:
    """
    ["HRANDFIELD","h","-2","WITHVALUES"]
    """
  Then the reply equals:
    """
    ["b","second","b","second"]
    """

@command_HRANDFIELD
Scenario: Positive random counts return every distinct field at most once
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["HSET","h","a","one","b","two"]]
    """
  When I execute:
    """
    ["HRANDFIELD","h","20"]
    """
  Then the reply contains:
    """
    ["a","b"]
    """
  And the reply has length 2
  When I execute:
    """
    ["HLEN","h"]
    """
  Then the reply equals:
    """
    2
    """
