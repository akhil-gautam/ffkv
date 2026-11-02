Feature: Redis 8.10 string commands
  String changes are atomic, binary safe, and preserve expiration when specified.

  @command_SET @command_GET
  Scenario: Value comparisons are atomic and failed SET GET returns the old value
    Given a fresh ffkv server
    When I execute:
      """
      ["SET","state","old"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["SET","state","new","IFEQ","wrong","GET"]
      """
    Then the reply equals:
      """
      "old"
      """
    When I execute:
      """
      ["GET","state"]
      """
    Then the reply equals:
      """
      "old"
      """
    When I execute:
      """
      ["SET","state","new","IFEQ","old"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["GET","state"]
      """
    Then the reply equals:
      """
      "new"
      """
    When I execute:
      """
      ["SET","absent","created","IFNE","anything"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["SET","missing","x","IFEQ","anything"]
      """
    Then the reply is null
    When I execute:
      """
      ["GET","missing"]
      """
    Then the reply is null

  @command_DIGEST
  Scenario: XXH3 digest matches the published vector and accepts uppercase comparisons
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["SET","message","Hello world"]]
      """
    When I execute:
      """
      ["DIGEST","message"]
      """
    Then the reply equals:
      """
      "b6acb9d84a38ff74"
      """
    When I execute:
      """
      ["DIGEST","missing"]
      """
    Then the reply is null
    When I execute:
      """
      ["SET","message","changed","IFDEQ","B6ACB9D84A38FF74"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["GET","message"]
      """
    Then the reply equals:
      """
      "changed"
      """
    When I execute:
      """
      ["SET","message","wrong","IFDNE","not-a-digest"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["GET","message"]
      """
    Then the reply equals:
      """
      "changed"
      """

  @command_DELEX
  Scenario: Conditional deletion cannot remove a changed value
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["SET","state","new"]]
      """
    When I execute:
      """
      ["DELEX","state","IFEQ","old"]
      """
    Then the reply equals:
      """
      0
      """
    When I execute:
      """
      ["GET","state"]
      """
    Then the reply equals:
      """
      "new"
      """
    When I execute:
      """
      ["DELEX","state","IFNE","old"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["DELEX","state"]
      """
    Then the reply equals:
      """
      0
      """
    When I execute:
      """
      ["DELEX","absent","IFDEQ","invalid"]
      """
    Then the reply equals:
      """
      0
      """

  @command_APPEND @command_STRLEN
  Scenario: Append creates then extends a byte string
    Given a fresh ffkv server
    When I execute:
      """
      ["APPEND","text","hello"]
      """
    Then the reply equals:
      """
      5
      """
    When I execute:
      """
      ["APPEND","text"," world"]
      """
    Then the reply equals:
      """
      11
      """
    When I execute:
      """
      ["STRLEN","text"]
      """
    Then the reply equals:
      """
      11
      """
    When I execute:
      """
      ["GET","text"]
      """
    Then the reply equals:
      """
      "hello world"
      """
    When I execute:
      """
      ["STRLEN","missing"]
      """
    Then the reply equals:
      """
      0
      """

  @command_DECR @command_DECRBY
  Scenario: Decrement uses zero for missing keys and rejects overflow
    Given a fresh ffkv server
    When I execute:
      """
      ["DECR","counter"]
      """
    Then the reply equals:
      """
      -1
      """
    When I execute:
      """
      ["DECRBY","counter","5"]
      """
    Then the reply equals:
      """
      -6
      """
    When I execute:
      """
      ["GET","counter"]
      """
    Then the reply equals:
      """
      "-6"
      """
    When I execute:
      """
      ["SET","counter","-9223372036854775808"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["DECR","counter"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["GET","counter"]
      """
    Then the reply equals:
      """
      "-9223372036854775808"
      """

  @command_INCR @command_INCRBY
  Scenario: Integer increments retain exact signed 64 bit values
    Given a fresh ffkv server
    When I execute:
      """
      ["INCR","counter"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["INCRBY","counter","9007199254740992"]
      """
    Then the reply equals:
      """
      9007199254740993
      """
    When I execute:
      """
      ["GET","counter"]
      """
    Then the reply equals:
      """
      "9007199254740993"
      """
    When I execute:
      """
      ["INCRBY","counter","1.5"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["GET","counter"]
      """
    Then the reply equals:
      """
      "9007199254740993"
      """

  @command_INCRBYFLOAT
  Scenario: Floating increments retain extended precision and TTL
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["SET","counter","9007199254740992","PXAT","4102444800000"]]
      """
    When I execute:
      """
      ["INCRBYFLOAT","counter","1"]
      """
    Then the reply equals:
      """
      "9007199254740993"
      """
    When I execute:
      """
      ["INCRBYFLOAT","counter","0.5"]
      """
    Then the reply equals:
      """
      "9007199254740993.5"
      """
    When I execute:
      """
      ["PEXPIRETIME","counter"]
      """
    Then the reply equals:
      """
      4102444800000
      """
    When I execute:
      """
      ["INCRBYFLOAT","counter","NaN"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["GET","counter"]
      """
    Then the reply equals:
      """
      "9007199254740993.5"
      """

  @command_INCREX
  Scenario: Bounded increments reject without changing expiration then saturate
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["SET","counter","99","PXAT","4102444800000"]]
      """
    When I execute:
      """
      ["INCREX","counter","BYINT","5","UBOUND","100","PERSIST"]
      """
    Then the reply equals:
      """
      [99,0]
      """
    When I execute:
      """
      ["GET","counter"]
      """
    Then the reply equals:
      """
      "99"
      """
    When I execute:
      """
      ["PEXPIRETIME","counter"]
      """
    Then the reply equals:
      """
      4102444800000
      """
    When I execute:
      """
      ["INCREX","counter","UBOUND","100","BYINT","5","SATURATE","PERSIST"]
      """
    Then the reply equals:
      """
      [100,1]
      """
    When I execute:
      """
      ["PTTL","counter"]
      """
    Then the reply equals:
      """
      -1
      """
    When I execute:
      """
      ["INCREX","counter","BYFLOAT","0.25"]
      """
    Then the reply equals:
      """
      ["100.25","0.25"]
      """
    When I execute:
      """
      ["INCREX","counter","BYINT","1"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["GET","counter"]
      """
    Then the reply equals:
      """
      "100.25"
      """

  @command_GETDEL
  Scenario: Read and delete removes both value and expiration
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["SET","item","payload","PXAT","4102444800000"]]
      """
    When I execute:
      """
      ["GETDEL","item"]
      """
    Then the reply equals:
      """
      "payload"
      """
    When I execute:
      """
      ["GETDEL","item"]
      """
    Then the reply is null
    When I execute:
      """
      ["PTTL","item"]
      """
    Then the reply equals:
      """
      -2
      """

  @command_GETEX
  Scenario: Getex preserves or changes expiration without changing bytes
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["SET","item","payload","PXAT","4102444800000"]]
      """
    When I execute:
      """
      ["GETEX","item"]
      """
    Then the reply equals:
      """
      "payload"
      """
    When I execute:
      """
      ["PEXPIRETIME","item"]
      """
    Then the reply equals:
      """
      4102444800000
      """
    When I execute:
      """
      ["GETEX","item","PERSIST"]
      """
    Then the reply equals:
      """
      "payload"
      """
    When I execute:
      """
      ["PTTL","item"]
      """
    Then the reply equals:
      """
      -1
      """
    When I execute:
      """
      ["GETEX","item","PXAT","1"]
      """
    Then the reply equals:
      """
      "payload"
      """
    When I execute:
      """
      ["GET","item"]
      """
    Then the reply is null

  @command_GETRANGE @command_SUBSTR
  Scenario: Range aliases use inclusive byte indexes and negative offsets
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["SET","text","abcdef"]]
      """
    When I execute:
      """
      ["GETRANGE","text","1","3"]
      """
    Then the reply equals:
      """
      "bcd"
      """
    When I execute:
      """
      ["SUBSTR","text","-3","-1"]
      """
    Then the reply equals:
      """
      "def"
      """
    When I execute:
      """
      ["GETRANGE","text","5","2"]
      """
    Then the reply equals:
      """
      ""
      """
    When I execute:
      """
      ["SUBSTR","missing","0","-1"]
      """
    Then the reply equals:
      """
      ""
      """

  @command_GETSET
  Scenario: Getset returns previous bytes and discards expiration
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["SET","item","old","PXAT","4102444800000"]]
      """
    When I execute:
      """
      ["GETSET","item","new"]
      """
    Then the reply equals:
      """
      "old"
      """
    When I execute:
      """
      ["GET","item"]
      """
    Then the reply equals:
      """
      "new"
      """
    When I execute:
      """
      ["PTTL","item"]
      """
    Then the reply equals:
      """
      -1
      """
    When I execute:
      """
      ["GETSET","missing","first"]
      """
    Then the reply is null
    When I execute:
      """
      ["GET","missing"]
      """
    Then the reply equals:
      """
      "first"
      """

  @command_LCS
  Scenario: Longest subsequence is not restricted to contiguous bytes
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["MSET","a","ohmytext","b","mynewtext"]]
      """
    When I execute:
      """
      ["LCS","a","b"]
      """
    Then the reply equals:
      """
      "mytext"
      """
    When I execute:
      """
      ["LCS","a","b","LEN"]
      """
    Then the reply equals:
      """
      6
      """
    When I execute:
      """
      ["LCS","a","b","IDX","MINMATCHLEN","4","WITHMATCHLEN"]
      """
    Then the reply equals:
      """
      ["matches",[[[4,7],[5,8],4]],"len",6]
      """
    When I execute:
      """
      ["LCS","a","b","LEN","IDX"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["GET","a"]
      """
    Then the reply equals:
      """
      "ohmytext"
      """

  @command_MSET @command_MGET
  Scenario: Multi key strings overwrite other types and MGET returns null for nonstrings
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["LPUSH","list","member"]]
      """
    When I execute:
      """
      ["MSET","a","one","b","two","a","last"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["MGET","b","absent","list","a"]
      """
    Then the reply equals:
      """
      ["two",null,null,"last"]
      """
    When I execute:
      """
      ["MSET","list","string"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["GET","list"]
      """
    Then the reply equals:
      """
      "string"
      """

  @command_MSETEX
  Scenario: Counted multi set evaluates NX atomically and shares one expiration
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["SET","existing","original"]]
      """
    When I execute:
      """
      ["MSETEX","2","existing","changed","new","new-value","NX","PXAT","4102444800000"]
      """
    Then the reply equals:
      """
      0
      """
    When I execute:
      """
      ["MGET","existing","new"]
      """
    Then the reply equals:
      """
      ["original",null]
      """
    When I execute:
      """
      ["MSETEX","2","existing","changed","new","new-value","XX"]
      """
    Then the reply equals:
      """
      0
      """
    When I execute:
      """
      ["MGET","existing","new"]
      """
    Then the reply equals:
      """
      ["original",null]
      """
    When I execute:
      """
      ["MSETEX","2","first","one","second","two","PXAT","4102444800000","NX"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["MGET","first","second"]
      """
    Then the reply equals:
      """
      ["one","two"]
      """
    When I execute:
      """
      ["PEXPIRETIME","first"]
      """
    Then the reply equals:
      """
      4102444800000
      """
    When I execute:
      """
      ["PEXPIRETIME","second"]
      """
    Then the reply equals:
      """
      4102444800000
      """
    When I execute:
      """
      ["MSETEX","2","first","x","second","y","XX","KEEPTTL"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["PEXPIRETIME","first"]
      """
    Then the reply equals:
      """
      4102444800000
      """
    When I execute:
      """
      ["MGET","first","second"]
      """
    Then the reply equals:
      """
      ["x","y"]
      """

  @command_MSETNX
  Scenario: Multi set NX checks original existence and last duplicate wins
    Given a fresh ffkv server
    When I execute:
      """
      ["MSETNX","a","one","a","last","b","two"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["MGET","a","b"]
      """
    Then the reply equals:
      """
      ["last","two"]
      """
    When I execute:
      """
      ["MSETNX","c","three","a","changed"]
      """
    Then the reply equals:
      """
      0
      """
    When I execute:
      """
      ["MGET","a","c"]
      """
    Then the reply equals:
      """
      ["last",null]
      """

  @command_PSETEX
  Scenario: Millisecond setex replaces value and expiry
    Given a fresh ffkv server
    When I execute:
      """
      ["PSETEX","item","3600000","payload"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["GET","item"]
      """
    Then the reply equals:
      """
      "payload"
      """
    When I execute:
      """
      ["PSETEX","item","0","changed"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["GET","item"]
      """
    Then the reply equals:
      """
      "payload"
      """

  @command_SETEX
  Scenario: Second setex validates before changing a value
    Given a fresh ffkv server
    When I execute:
      """
      ["SETEX","item","3600","payload"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["GET","item"]
      """
    Then the reply equals:
      """
      "payload"
      """
    When I execute:
      """
      ["SETEX","item","-1","changed"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["GET","item"]
      """
    Then the reply equals:
      """
      "payload"
      """

  @command_SETNX
  Scenario: Set NX only creates missing keys
    Given a fresh ffkv server
    When I execute:
      """
      ["SETNX","item","first"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["SETNX","item","second"]
      """
    Then the reply equals:
      """
      0
      """
    When I execute:
      """
      ["GET","item"]
      """
    Then the reply equals:
      """
      "first"
      """

  @command_SETRANGE
  Scenario: Sparse writes zero fill holes and retain TTL
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["SET","text","abc","PXAT","4102444800000"]]
      """
    When I execute:
      """
      ["SETRANGE","text","1","Z"]
      """
    Then the reply equals:
      """
      3
      """
    When I execute:
      """
      ["GET","text"]
      """
    Then the reply equals:
      """
      "aZc"
      """
    When I execute:
      """
      ["SETRANGE","text","5","x"]
      """
    Then the reply equals:
      """
      6
      """
    When I execute:
      """
      ["GET","text"]
      """
    Then the reply equals:
      """
      "aZc\u0000\u0000x"
      """
    When I execute:
      """
      ["PEXPIRETIME","text"]
      """
    Then the reply equals:
      """
      4102444800000
      """
    When I execute:
      """
      ["SETRANGE","text","-1","bad"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["GET","text"]
      """
    Then the reply equals:
      """
      "aZc\u0000\u0000x"
      """

  @command_SET @command_DELEX
  Scenario: Value comparisons are binary safe and digest equality can delete a key
    Given a fresh ffkv server
    When I execute:
      """
      ["SET",{"base64":"AP8="},{"base64":"/wAB"}]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["SET",{"base64":"AP8="},"changed","IFEQ",{"base64":"/wAB"}]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["GET",{"base64":"AP8="}]
      """
    Then the reply equals:
      """
      "changed"
      """
    When I execute:
      """
      ["SET","digest-delete","Hello world"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["DELEX","digest-delete","IFDEQ","B6ACB9D84A38FF74"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["GET","digest-delete"]
      """
    Then the reply is null

  @command_INCREX
  Scenario: Duplicate options and unrepresentable saturated delta do not mutate
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["SET","counter","9223372036854775800","PXAT","4102444800000"]]
      """
    When I execute:
      """
      ["INCREX","counter","BYINT","9223372036854775800","SATURATE"]
      """
    Then the reply equals:
      """
      [9223372036854775807,7]
      """
    When I execute:
      """
      ["INCREX","counter","SATURATE","SATURATE"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["INCREX","counter","BYINT","1","UBOUND","-9223372036854775808","SATURATE"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["GET","counter"]
      """
    Then the reply equals:
      """
      "9223372036854775807"
      """
    When I execute:
      """
      ["PEXPIRETIME","counter"]
      """
    Then the reply equals:
      """
      4102444800000
      """

  @command_MSETEX @command_INCREX @command_DELEX @command_INCRBYFLOAT
  Scenario: Resolved multi string writes survive crash recovery with their expiration
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["MSETEX","3","counter","99","delete-me","old","float","1.5","PXAT","4102444800000"]]
      """
    When I execute:
      """
      ["INCREX","counter","BYINT","5","UBOUND","100","SATURATE"]
      """
    Then the reply equals:
      """
      [100,1]
      """
    When I execute:
      """
      ["DELEX","delete-me","IFEQ","old"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["INCRBYFLOAT","float","0.25"]
      """
    Then the reply equals:
      """
      "1.75"
      """
    When ffkv restarts after a crash
    When I execute:
      """
      ["MGET","counter","delete-me","float"]
      """
    Then the reply equals:
      """
      ["100",null,"1.75"]
      """
    When I execute:
      """
      ["PEXPIRETIME","counter"]
      """
    Then the reply equals:
      """
      4102444800000
      """

  @command_INCREX
  Scenario: An applied bounded increment invalidates a watching transaction
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["SET","counter","5"]]
      """
    When client "watcher" executes:
      """
      ["WATCH","counter"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["INCREX","counter","BYINT","1","UBOUND","10"]
      """
    Then the reply equals:
      """
      [6,1]
      """
    When client "watcher" executes:
      """
      ["MULTI"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "watcher" executes:
      """
      ["GET","counter"]
      """
    Then the reply equals:
      """
      "QUEUED"
      """
    When client "watcher" executes:
      """
      ["EXEC"]
      """
    Then the reply is null

  @command_DIGEST @command_DELEX @command_INCRBYFLOAT @command_INCREX @command_LCS
  @command_GET @command_GETDEL @command_GETEX @command_GETSET @command_APPEND
  @command_STRLEN @command_GETRANGE @command_SUBSTR @command_SETRANGE @command_SET
  Scenario Outline: String type errors never destroy a collection
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["LPUSH","typed","member"]]
      """
    When I execute:
      """
      <argv>
      """
    Then the reply is an error with code "<code>"
    When I execute:
      """
      ["LRANGE","typed","0","-1"]
      """
    Then the reply equals:
      """
      ["member"]
      """
    Examples:
      | argv                                     | code      |
      | ["DIGEST","typed"]                        | WRONGTYPE |
      | ["DELEX","typed","IFEQ","member"]          | ERR       |
      | ["INCRBYFLOAT","typed","1.5"]              | WRONGTYPE |
      | ["INCREX","typed"]                        | WRONGTYPE |
      | ["LCS","typed","missing"]                 | WRONGTYPE |
      | ["GET","typed"]                           | WRONGTYPE |
      | ["GETDEL","typed"]                        | WRONGTYPE |
      | ["GETEX","typed","PERSIST"]               | WRONGTYPE |
      | ["GETSET","typed","replacement"]          | WRONGTYPE |
      | ["APPEND","typed","suffix"]               | WRONGTYPE |
      | ["STRLEN","typed"]                        | WRONGTYPE |
      | ["GETRANGE","typed","0","-1"]             | WRONGTYPE |
      | ["SUBSTR","typed","0","-1"]               | WRONGTYPE |
      | ["SETRANGE","typed","0","replacement"]    | WRONGTYPE |
      | ["SET","typed","replacement","GET"]       | WRONGTYPE |
      | ["SET","typed","replacement","IFNE","x"]  | WRONGTYPE |

  @command_MSETEX
  Scenario: Counted setters handle aliases before mutation and past absolute deadlines
    Given a fresh ffkv server
    When I execute:
      """
      ["MSETEX","2","alias","first","alias","last","NX","PXAT","4102444800000"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["GET","alias"]
      """
    Then the reply equals:
      """
      "last"
      """
    When I execute:
      """
      ["MSETEX","2","alias","next","alias","final","XX","KEEPTTL"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["PEXPIRETIME","alias"]
      """
    Then the reply equals:
      """
      4102444800000
      """
    When I execute:
      """
      ["MSETEX","2","alias","bad","other","bad","EX","0"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["MGET","alias","other"]
      """
    Then the reply equals:
      """
      ["final",null]
      """
    When I execute:
      """
      ["MSETEX","2","alias","gone","other","gone","PXAT","1"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["MGET","alias","other"]
      """
    Then the reply equals:
      """
      [null,null]
      """
