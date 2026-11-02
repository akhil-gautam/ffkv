Feature: Redis generic keyspace commands
  Key movement preserves values and expiration, and failed replacements are atomic.

  @command_COPY
  Scenario: Copy across databases replaces only when requested and detaches hash values and field deadlines
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["HSET","source","expiring","original","persistent","kept"],["HPEXPIREAT","source",4102444800123,"FIELDS",1,"expiring"],["PEXPIREAT","source",4102444800999],["SELECT",1],["SET","copy","occupied"],["SELECT",0]]
      """
    When I execute:
      """
      ["COPY","source","copy","DB",1]
      """
    Then the reply equals:
      """
      0
      """
    When I execute:
      """
      ["SELECT",1]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["GET","copy"]
      """
    Then the reply equals:
      """
      "occupied"
      """
    When I execute:
      """
      ["SELECT",0]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["COPY","source","copy","DB",1,"REPLACE"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["SELECT",1]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["HMGET","copy","expiring","persistent"]
      """
    Then the reply equals:
      """
      ["original","kept"]
      """
    When I execute:
      """
      ["PEXPIRETIME","copy"]
      """
    Then the reply equals:
      """
      4102444800999
      """
    When I execute:
      """
      ["HPEXPIRETIME","copy","FIELDS",2,"expiring","persistent"]
      """
    Then the reply equals:
      """
      [4102444800123,-1]
      """
    Given the following commands succeed:
      """
      [["HSET","copy","expiring","changed"],["HDEL","copy","persistent"],["PERSIST","copy"],["SELECT",0]]
      """
    When I execute:
      """
      ["HMGET","source","expiring","persistent"]
      """
    Then the reply equals:
      """
      ["original","kept"]
      """
    When I execute:
      """
      ["HPEXPIRETIME","source","FIELDS",1,"expiring"]
      """
    Then the reply equals:
      """
      [4102444800123]
      """
    When I execute:
      """
      ["PEXPIRETIME","source"]
      """
    Then the reply equals:
      """
      4102444800999
      """
    When I execute:
      """
      ["COPY","missing","not-created"]
      """
    Then the reply equals:
      """
      0
      """
    When I execute:
      """
      ["EXISTS","not-created"]
      """
    Then the reply equals:
      """
      0
      """

  @command_DEL @command_EXISTS @command_UNLINK
  Scenario: Existence counts repeated arguments but deletion counts each removed key once
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["SET","text","value"],["RPUSH","list","one","two"],["HSET","hash","field","value"]]
      """
    When I execute:
      """
      ["EXISTS","text","text","list","missing"]
      """
    Then the reply equals:
      """
      3
      """
    When I execute:
      """
      ["DEL","text","text","missing","list"]
      """
    Then the reply equals:
      """
      2
      """
    When I execute:
      """
      ["EXISTS","text","list","hash"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["UNLINK","hash","hash","missing"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["HGET","hash","field"]
      """
    Then the reply is null
    When I execute:
      """
      ["UNLINK","hash"]
      """
    Then the reply equals:
      """
      0
      """

  @command_DUMP @command_RESTORE
  Scenario: Dump and restore round trip binary values without copying the source key deadline
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["SET","binary",{"base64":"AP8="}],["PEXPIREAT","binary",4102444800123]]
      """
    When I execute:
      """
      ["DUMP","binary"]
      """
    When I save the reply as "binary-dump"
    When I execute:
      """
      ["RESTORE","restored",0,{"saved":"binary-dump"}]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["GET","restored"]
      """
    Then the reply equals:
      """
      {"type":"bytes","base64":"AP8="}
      """
    When I execute:
      """
      ["PTTL","restored"]
      """
    Then the reply equals:
      """
      -1
      """
    When I execute:
      """
      ["RESTORE","restored",4102444800456,{"saved":"binary-dump"},"ABSTTL","REPLACE"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["PEXPIRETIME","restored"]
      """
    Then the reply equals:
      """
      4102444800456
      """
    When I execute:
      """
      ["SET","binary","changed"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["GET","restored"]
      """
    Then the reply equals:
      """
      {"type":"bytes","base64":"AP8="}
      """
    When I execute:
      """
      ["DUMP","missing"]
      """
    Then the reply is null

  @command_RESTORE @command_DUMP
  Scenario: Invalid serialized data cannot replace a live value or its deadline
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["SET","target","retained"],["PEXPIREAT","target",4102444800123],["SET","source","replacement"]]
      """
    When I execute:
      """
      ["RESTORE","target",0,{"base64":"AAAA"},"REPLACE"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["GET","target"]
      """
    Then the reply equals:
      """
      "retained"
      """
    When I execute:
      """
      ["PEXPIRETIME","target"]
      """
    Then the reply equals:
      """
      4102444800123
      """
    When I execute:
      """
      ["DUMP","source"]
      """
    When I save the reply as "replacement-dump"
    When I execute:
      """
      ["RESTORE","target",0,{"saved":"replacement-dump"}]
      """
    Then the reply is an error with code "BUSYKEY"
    When I execute:
      """
      ["GET","target"]
      """
    Then the reply equals:
      """
      "retained"
      """
    When I execute:
      """
      ["RESTORE","absent",0,{"base64":"AAAA"}]
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

  @command_DUMP @command_RESTORE
  Scenario: Hash serialization preserves binary fields and absolute field expiration independently
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["HSET","hash",{"base64":"AP8="},"binary-field","timed","value","plain","persistent"],["HPEXPIREAT","hash",4102444800123,"FIELDS",1,"timed"]]
      """
    When I execute:
      """
      ["DUMP","hash"]
      """
    When I save the reply as "hash-dump"
    When I execute:
      """
      ["RESTORE","hash-copy",0,{"saved":"hash-dump"}]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["HMGET","hash-copy",{"base64":"AP8="},"timed","plain"]
      """
    Then the reply equals:
      """
      ["binary-field","value","persistent"]
      """
    When I execute:
      """
      ["HPEXPIRETIME","hash-copy","FIELDS",2,"timed","plain"]
      """
    Then the reply equals:
      """
      [4102444800123,-1]
      """
    When I execute:
      """
      ["HSET","hash-copy","timed","changed"]
      """
    Then the reply equals:
      """
      0
      """
    When I execute:
      """
      ["HGET","hash","timed"]
      """
    Then the reply equals:
      """
      "value"
      """
    When I execute:
      """
      ["HPEXPIRETIME","hash","FIELDS",1,"timed"]
      """
    Then the reply equals:
      """
      [4102444800123]
      """

  @command_EXPIRE @command_TTL @command_PERSIST
  Scenario: Relative seconds deadlines honor NX XX GT LT and persistence transitions
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["SET","key","value"]]
      """
    When I execute:
      """
      ["TTL","key"]
      """
    Then the reply equals:
      """
      -1
      """
    When I execute:
      """
      ["EXPIRE","key",3600,"XX"]
      """
    Then the reply equals:
      """
      0
      """
    When I execute:
      """
      ["EXPIRE","key",3600,"GT"]
      """
    Then the reply equals:
      """
      0
      """
    When I execute:
      """
      ["EXPIRE","key",3600,"NX"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["TTL","key"]
      """
    Then the reply is between 3500 and 3600
    When I execute:
      """
      ["EXPIRE","key",7200,"NX"]
      """
    Then the reply equals:
      """
      0
      """
    When I execute:
      """
      ["EXPIRE","key",7200,"XX","GT"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["EXPIRE","key",10800,"LT"]
      """
    Then the reply equals:
      """
      0
      """
    When I execute:
      """
      ["EXPIRE","key",1800,"XX","LT"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["PERSIST","key"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["TTL","key"]
      """
    Then the reply equals:
      """
      -1
      """
    When I execute:
      """
      ["PERSIST","key"]
      """
    Then the reply equals:
      """
      0
      """
    When I execute:
      """
      ["PERSIST","missing"]
      """
    Then the reply equals:
      """
      0
      """
    When I execute:
      """
      ["EXPIRE","key",0]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["GET","key"]
      """
    Then the reply is null
    When I execute:
      """
      ["TTL","key"]
      """
    Then the reply equals:
      """
      -2
      """
    When I execute:
      """
      ["EXPIRE","key",3600]
      """
    Then the reply equals:
      """
      0
      """

  @command_PEXPIRE @command_PTTL
  Scenario: Millisecond expiration conditions preserve live data until an immediate deadline
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["SET","key","value"]]
      """
    When I execute:
      """
      ["PTTL","key"]
      """
    Then the reply equals:
      """
      -1
      """
    When I execute:
      """
      ["PEXPIRE","key",3600000,"XX"]
      """
    Then the reply equals:
      """
      0
      """
    When I execute:
      """
      ["PEXPIRE","key",3600000,"NX"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["PTTL","key"]
      """
    Then the reply is between 3500000 and 3600000
    When I execute:
      """
      ["PEXPIRE","key",7200000,"NX"]
      """
    Then the reply equals:
      """
      0
      """
    When I execute:
      """
      ["PEXPIRE","key",7200000,"GT"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["PEXPIRE","key",10800000,"LT"]
      """
    Then the reply equals:
      """
      0
      """
    When I execute:
      """
      ["PEXPIRE","key",1800000,"XX","LT"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["GET","key"]
      """
    Then the reply equals:
      """
      "value"
      """
    When I execute:
      """
      ["PEXPIRE","key",-1]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["PTTL","key"]
      """
    Then the reply equals:
      """
      -2
      """
    When I execute:
      """
      ["PEXPIRE","missing",3600000]
      """
    Then the reply equals:
      """
      0
      """

  @command_EXPIREAT @command_EXPIRETIME @command_PEXPIREAT @command_PEXPIRETIME
  Scenario: Absolute deadlines report exact units and conditionally replace deadlines
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["SET","key","value"]]
      """
    When I execute:
      """
      ["EXPIRETIME","key"]
      """
    Then the reply equals:
      """
      -1
      """
    When I execute:
      """
      ["PEXPIRETIME","missing"]
      """
    Then the reply equals:
      """
      -2
      """
    When I execute:
      """
      ["EXPIREAT","key",4102444800,"NX"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["EXPIRETIME","key"]
      """
    Then the reply equals:
      """
      4102444800
      """
    When I execute:
      """
      ["PEXPIRETIME","key"]
      """
    Then the reply equals:
      """
      4102444800000
      """
    When I execute:
      """
      ["EXPIREAT","key",4102444801,"NX"]
      """
    Then the reply equals:
      """
      0
      """
    When I execute:
      """
      ["EXPIREAT","key",4102444801,"XX","GT"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["PEXPIREAT","key",4102444800123,"XX","LT"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["PEXPIRETIME","key"]
      """
    Then the reply equals:
      """
      4102444800123
      """
    When I execute:
      """
      ["PEXPIREAT","key",4102444800999,"LT"]
      """
    Then the reply equals:
      """
      0
      """
    When I execute:
      """
      ["EXPIREAT","key",1]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["EXPIRETIME","key"]
      """
    Then the reply equals:
      """
      -2
      """
    When I execute:
      """
      ["PEXPIREAT","key",4102444800123]
      """
    Then the reply equals:
      """
      0
      """

  @command_KEYS
  Scenario: Key patterns support character classes wildcards and escaped metacharacters
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["SET","item:1","one"],["SET","item:2","two"],["SET","literal*","star"],["SET","other","other"]]
      """
    When I execute:
      """
      ["KEYS","item:[1]?"]
      """
    Then the reply equals:
      """
      []
      """
    When I execute:
      """
      ["KEYS","item:[1]"]
      """
    Then the reply equals:
      """
      ["item:1"]
      """
    When I execute:
      """
      ["KEYS","item:?"]
      """
    Then the reply contains:
      """
      ["item:1","item:2"]
      """
    When I execute:
      """
      ["KEYS","literal\\*"]
      """
    Then the reply equals:
      """
      ["literal*"]
      """

  @command_MOVE
  Scenario: Move respects destination collisions and transfers a hash with its deadline to another database
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["HSET","moving","field","source"],["PEXPIREAT","moving",4102444800123],["SELECT",2],["SET","moving","occupied"],["SELECT",0]]
      """
    When I execute:
      """
      ["MOVE","moving",2]
      """
    Then the reply equals:
      """
      0
      """
    When I execute:
      """
      ["HGET","moving","field"]
      """
    Then the reply equals:
      """
      "source"
      """
    Given the following commands succeed:
      """
      [["SELECT",2]]
      """
    When I execute:
      """
      ["GET","moving"]
      """
    Then the reply equals:
      """
      "occupied"
      """
    Given the following commands succeed:
      """
      [["DEL","moving"],["SELECT",0]]
      """
    When I execute:
      """
      ["MOVE","moving",2]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["EXISTS","moving"]
      """
    Then the reply equals:
      """
      0
      """
    Given the following commands succeed:
      """
      [["SELECT",2]]
      """
    When I execute:
      """
      ["HGET","moving","field"]
      """
    Then the reply equals:
      """
      "source"
      """
    When I execute:
      """
      ["PEXPIRETIME","moving"]
      """
    Then the reply equals:
      """
      4102444800123
      """
    When I execute:
      """
      ["MOVE","missing",0]
      """
    Then the reply equals:
      """
      0
      """

  @command_OBJECT_ENCODING @command_OBJECT_REFCOUNT
  Scenario: Object representation metadata describes a live value and is absent for nonexistent keys
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["SET","object","a nonnumeric value whose representation is implementation dependent"]]
      """
    When I execute:
      """
      ["OBJECT","ENCODING","object"]
      """
    When I save the reply as "string-encoding"
    And the following commands succeed:
      """
      [["HSET","hash-object","field","value"]]
      """
    When I execute:
      """
      ["OBJECT","ENCODING","hash-object"]
      """
    Then the reply does not equal the saved value "string-encoding"
    When I execute:
      """
      ["OBJECT","REFCOUNT","object"]
      """
    Then the reply is between 1 and 2147483647
    When I execute:
      """
      ["TYPE","object"]
      """
    Then the reply equals:
      """
      "string"
      """
    When I execute:
      """
      ["GET","object"]
      """
    Then the reply equals:
      """
      "a nonnumeric value whose representation is implementation dependent"
      """
    When I execute:
      """
      ["OBJECT","ENCODING","missing"]
      """
    Then the reply is null
    When I execute:
      """
      ["OBJECT","REFCOUNT","missing"]
      """
    Then the reply is null

  @command_OBJECT_IDLETIME @command_TOUCH
  Scenario: Touch refreshes actual restored idle metadata without changing values or deadlines
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["SET","seed","value"]]
      """
    When I execute:
      """
      ["DUMP","seed"]
      """
    When I save the reply as "idle-dump"
    When I execute:
      """
      ["RESTORE","idle",4102444800123,{"saved":"idle-dump"},"ABSTTL","IDLETIME",120]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["OBJECT","IDLETIME","idle"]
      """
    Then the reply is between 120 and 150
    When I execute:
      """
      ["TOUCH","idle","missing"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["OBJECT","IDLETIME","idle"]
      """
    Then the reply is between 0 and 30
    When I execute:
      """
      ["GET","idle"]
      """
    Then the reply equals:
      """
      "value"
      """
    When I execute:
      """
      ["PEXPIRETIME","idle"]
      """
    Then the reply equals:
      """
      4102444800123
      """
    When I execute:
      """
      ["OBJECT","IDLETIME","missing"]
      """
    Then the reply is null

  @command_OBJECT_FREQ
  Scenario: Actual reads advance restored access frequency metadata
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["SET","seed","value"]]
      """
    When I execute:
      """
      ["DUMP","seed"]
      """
    When I save the reply as "frequency-dump"
    When I execute:
      """
      ["RESTORE","frequent",0,{"saved":"frequency-dump"},"FREQ",42]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["OBJECT","FREQ","frequent"]
      """
    Then the reply equals:
      """
      42
      """
    When I execute:
      """
      ["GET","frequent"]
      """
    Then the reply equals:
      """
      "value"
      """
    When I execute:
      """
      ["OBJECT","FREQ","frequent"]
      """
    Then the reply is between 43 and 255
    When I execute:
      """
      ["OBJECT","FREQ","missing"]
      """
    Then the reply is null

  @command_RANDOMKEY
  Scenario: Random key returns the only live key and becomes null after deletion
    Given a fresh ffkv server
    When I execute:
      """
      ["RANDOMKEY"]
      """
    Then the reply is null
    Given the following commands succeed:
      """
      [["SET","only","value"]]
      """
    When I execute:
      """
      ["RANDOMKEY"]
      """
    Then the reply equals:
      """
      "only"
      """
    Given the following commands succeed:
      """
      [["DEL","only"]]
      """
    When I execute:
      """
      ["RANDOMKEY"]
      """
    Then the reply is null

  @command_RENAME
  Scenario: Rename overwrites the destination and preserves the source collection and expiration
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["RPUSH","source","first","second"],["PEXPIREAT","source",4102444800123],["SET","destination","old"],["PEXPIREAT","destination",4102444800999]]
      """
    When I execute:
      """
      ["RENAME","source","destination"]
      """
    Then the reply equals:
      """
      "OK"
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
      ["LRANGE","destination",0,-1]
      """
    Then the reply equals:
      """
      ["first","second"]
      """
    When I execute:
      """
      ["PEXPIRETIME","destination"]
      """
    Then the reply equals:
      """
      4102444800123
      """
    When I execute:
      """
      ["RENAME","destination","destination"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["RENAME","missing","destination"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["LRANGE","destination",0,-1]
      """
    Then the reply equals:
      """
      ["first","second"]
      """

  @command_RENAMENX
  Scenario: Conditional rename retains both keys on collision and moves only to an absent destination
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["SET","source","new"],["PEXPIREAT","source",4102444800123],["SET","destination","old"]]
      """
    When I execute:
      """
      ["RENAMENX","source","destination"]
      """
    Then the reply equals:
      """
      0
      """
    When I execute:
      """
      ["MGET","source","destination"]
      """
    Then the reply equals:
      """
      ["new","old"]
      """
    When I execute:
      """
      ["RENAMENX","source","source"]
      """
    Then the reply equals:
      """
      0
      """
    When I execute:
      """
      ["RENAMENX","source","available"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["MGET","source","available","destination"]
      """
    Then the reply equals:
      """
      [null,"new","old"]
      """
    When I execute:
      """
      ["PEXPIRETIME","available"]
      """
    Then the reply equals:
      """
      4102444800123
      """

  @command_SCAN
  Scenario: A complete singleton scan supports MATCH COUNT TYPE and a terminal empty result
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["RPUSH","queue:only","item"]]
      """
    When I execute:
      """
      ["SCAN",0,"MATCH","queue:*","COUNT",10000,"TYPE","list"]
      """
    Then the reply equals:
      """
      ["0",["queue:only"]]
      """
    When I execute:
      """
      ["SCAN",0,"MATCH","queue:*","COUNT",10000,"TYPE","string"]
      """
    Then the reply equals:
      """
      ["0",[]]
      """
    When I execute:
      """
      ["SCAN",0,"MATCH","other:*","COUNT",10000]
      """
    Then the reply equals:
      """
      ["0",[]]
      """
    Given the following commands succeed:
      """
      [["DEL","queue:only"]]
      """
    When I execute:
      """
      ["SCAN",0]
      """
    Then the reply equals:
      """
      ["0",[]]
      """

  @command_SORT
  Scenario: Sort resolves external hash weights and multiple GET patterns before limiting and storing
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["RPUSH","ids","a","c","b"],["HSET","item:a","rank",2,"label","Alpha"],["HSET","item:b","rank",3,"label","Beta"],["HSET","item:c","rank",1,"label","Gamma"],["SET","result","replaced"],["PEXPIREAT","result",4102444800123]]
      """
    When I execute:
      """
      ["SORT","ids","BY","item:*->rank","GET","#","GET","item:*->label","DESC","LIMIT",1,2]
      """
    Then the reply equals:
      """
      ["a","Alpha","c","Gamma"]
      """
    When I execute:
      """
      ["SORT","ids","BY","item:*->rank","GET","#","GET","item:*->label","DESC","LIMIT",1,2,"STORE","result"]
      """
    Then the reply equals:
      """
      4
      """
    When I execute:
      """
      ["LRANGE","result",0,-1]
      """
    Then the reply equals:
      """
      ["a","Alpha","c","Gamma"]
      """
    When I execute:
      """
      ["PTTL","result"]
      """
    Then the reply equals:
      """
      -1
      """
    When I execute:
      """
      ["LRANGE","ids",0,-1]
      """
    Then the reply equals:
      """
      ["a","c","b"]
      """
    When I execute:
      """
      ["SORT","ids","ALPHA","DESC","LIMIT",0,2]
      """
    Then the reply equals:
      """
      ["c","b"]
      """
    When I execute:
      """
      ["SORT","ids","BY","item:*->rank","GET","absent:*"]
      """
    Then the reply equals:
      """
      [null,null,null]
      """

  @command_SORT
  Scenario: A failed numeric sort cannot alter its store destination or the original collection
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["RPUSH","numbers","2","not-a-number","1"],["RPUSH","result","retained"],["PEXPIREAT","result",4102444800123]]
      """
    When I execute:
      """
      ["SORT","numbers","STORE","result"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["LRANGE","result",0,-1]
      """
    Then the reply equals:
      """
      ["retained"]
      """
    When I execute:
      """
      ["PEXPIRETIME","result"]
      """
    Then the reply equals:
      """
      4102444800123
      """
    When I execute:
      """
      ["LRANGE","numbers",0,-1]
      """
    Then the reply equals:
      """
      ["2","not-a-number","1"]
      """

  @command_SORT_RO
  Scenario: Read only sorting resolves external keys and leaves an expiring set unchanged
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["SADD","ids","3","1","2"],["SET","weight:1",30],["SET","weight:2",10],["SET","weight:3",20],["PEXPIREAT","ids",4102444800123]]
      """
    When I execute:
      """
      ["SORT_RO","ids","BY","weight:*","GET","#","GET","weight:*","DESC","LIMIT",0,2]
      """
    Then the reply equals:
      """
      ["1","30","3","20"]
      """
    When I execute:
      """
      ["SORT_RO","ids"]
      """
    Then the reply equals:
      """
      ["1","2","3"]
      """
    When I execute:
      """
      ["TYPE","ids"]
      """
    Then the reply equals:
      """
      "set"
      """
    When I execute:
      """
      ["SCARD","ids"]
      """
    Then the reply equals:
      """
      3
      """
    When I execute:
      """
      ["PEXPIRETIME","ids"]
      """
    Then the reply equals:
      """
      4102444800123
      """

  @command_TYPE
  Scenario: Type distinguishes built in values and missing keys without inspecting their contents
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["SET","text","value"],["RPUSH","list","value"],["HSET","hash","field","value"],["SADD","set","value"],["ZADD","sorted",1,"value"]]
      """
    When I execute:
      """
      ["TYPE","text"]
      """
    Then the reply equals:
      """
      "string"
      """
    When I execute:
      """
      ["TYPE","list"]
      """
    Then the reply equals:
      """
      "list"
      """
    When I execute:
      """
      ["TYPE","hash"]
      """
    Then the reply equals:
      """
      "hash"
      """
    When I execute:
      """
      ["TYPE","set"]
      """
    Then the reply equals:
      """
      "set"
      """
    When I execute:
      """
      ["TYPE","sorted"]
      """
    Then the reply equals:
      """
      "zset"
      """
    When I execute:
      """
      ["TYPE","missing"]
      """
    Then the reply equals:
      """
      "none"
      """

  @command_MIGRATE
  Scenario: Migration copies into a real target database and removes the source only for a move
    Given a fresh ffkv server
    And another fresh ffkv server named "target"
    And the following commands succeed:
      """
      [["HSET","transfer","field","original"],["EXPIRE","transfer",3600]]
      """
    When I execute:
      """
      ["MIGRATE","{{target_host}}","{{target_port}}","transfer",2,5000,"COPY"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["HGET","transfer","field"]
      """
    Then the reply equals:
      """
      "original"
      """
    When server "target" executes:
      """
      ["SELECT",2]
      """
    Then the reply equals:
      """
      "OK"
      """
    When server "target" executes:
      """
      ["HGET","transfer","field"]
      """
    Then the reply equals:
      """
      "original"
      """
    When server "target" executes:
      """
      ["PTTL","transfer"]
      """
    Then the reply is between 3500000 and 3600000
    When I execute:
      """
      ["HSET","transfer","field","changed"]
      """
    Then the reply equals:
      """
      0
      """
    When server "target" executes:
      """
      ["HGET","transfer","field"]
      """
    Then the reply equals:
      """
      "original"
      """
    When I execute:
      """
      ["MIGRATE","{{target_host}}","{{target_port}}","transfer",2,5000,"REPLACE"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["EXISTS","transfer"]
      """
    Then the reply equals:
      """
      0
      """
    When server "target" executes:
      """
      ["HGET","transfer","field"]
      """
    Then the reply equals:
      """
      "changed"
      """
    When server "target" executes:
      """
      ["PTTL","transfer"]
      """
    Then the reply is between 3500000 and 3600000

  @command_WAIT
  Scenario: Waiting for an unavailable replica reports zero without losing the preceding write
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["SET","written","value"]]
      """
    When I execute:
      """
      ["WAIT",1,1]
      """
    Then the reply equals:
      """
      0
      """
    When I execute:
      """
      ["GET","written"]
      """
    Then the reply equals:
      """
      "value"
      """

  @command_WAITAOF
  Scenario: Waiting for local durability acknowledges the preceding write without replicas
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["SET","durable","value"]]
      """
    When I execute:
      """
      ["WAITAOF",1,0,5000]
      """
    Then the reply equals:
      """
      [1,0]
      """
    When ffkv restarts after a crash
    When I execute:
      """
      ["GET","durable"]
      """
    Then the reply equals:
      """
      "value"
      """
