@client_control_repair
Feature: Client controls preserve cancellation, disposable state and authorization evidence

  @command_WAIT @command_CLIENT_UNBLOCK
  Scenario Outline: Replication WAIT is unblocked by exact client identity
    Given a fresh ffkv server
    When client "waiting" executes:
      """
      ["CLIENT","ID"]
      """
    When I save the reply as "wait-id"
    When client "waiting" sends:
      """
      ["WAIT",1,0]
      """
    Then client "waiting" is blocked
    When I execute:
      """
      ["CLIENT","UNBLOCK",9223372036854775807,"ERROR"]
      """
    Then the reply equals:
      """
      0
      """
    Then client "waiting" has no reply for 30 milliseconds
    When I execute:
      """
      ["CLIENT","UNBLOCK",{"saved":"wait-id"},"<mode>"]
      """
    Then the reply equals:
      """
      1
      """
    Then client "waiting" receives:
      """
      <reply>
      """
    When I execute:
      """
      ["CLIENT","UNBLOCK",{"saved":"wait-id"},"ERROR"]
      """
    Then the reply equals:
      """
      0
      """
    When client "waiting" executes:
      """
      ["PING"]
      """
    Then the reply equals:
      """
      "PONG"
      """
    Examples:
      | mode    | reply                                                                                         |
      | TIMEOUT | 0                                                                                             |
      | ERROR   | {"type":"error","value":"UNBLOCKED client unblocked via CLIENT UNBLOCK"}                     |

  @command_WAITAOF @command_CLIENT_UNBLOCK
  Scenario Outline: Durable replication waits expose current counts or explicit cancellation
    Given a fresh ffkv server
    When client "durable" executes:
      """
      ["CLIENT","ID"]
      """
    When I save the reply as "durable-id"
    When client "durable" sends:
      """
      ["WAITAOF",0,1,0]
      """
    Then client "durable" is blocked
    When I execute:
      """
      ["CLIENT","UNBLOCK",{"saved":"durable-id"},"<mode>"]
      """
    Then the reply equals:
      """
      1
      """
    Then client "durable" receives:
      """
      <reply>
      """
    When client "durable" executes:
      """
      ["PING"]
      """
    Then the reply equals:
      """
      "PONG"
      """
    Examples:
      | mode    | reply                                                                                         |
      | TIMEOUT | [1,0]                                                                                         |
      | ERROR   | {"type":"error","value":"UNBLOCKED client unblocked via CLIENT UNBLOCK"}                     |

  @command_CLIENT_PAUSE @command_CLIENT_UNPAUSE @command_PUBLISH @command_SPUBLISH @command_PFCOUNT
  Scenario Outline: WRITE pauses hold may-replicate operations without holding reads
    Given a fresh ffkv server
    When I execute:
      """
      ["CLIENT","PAUSE",10000,"WRITE"]
      """
    When client "publisher" sends:
      """
      <command>
      """
    Then client "publisher" has no reply for 50 milliseconds
    When I execute:
      """
      ["GET","not-a-key"]
      """
    Then the reply is null
    When I execute:
      """
      ["CLIENT","UNPAUSE"]
      """
    Then client "publisher" receives:
      """
      0
      """
    Examples:
      | command                        |
      | ["PUBLISH","news","message"]   |
      | ["SPUBLISH","news","message"]  |
      | ["PFCOUNT","missing-hll"]       |

  @command_WAIT @command_WAITAOF @command_CLIENT_UNBLOCK @native_replication
  Scenario Outline: TIMEOUT unblocking preserves acknowledgments already received from a replica
    Given an owned synchronized native replication pair
    When node "master" executes:
      """
      ["SET","acknowledged-key","value"]
      """
    When node "master" executes:
      """
      ["WAITAOF",1,1,2000]
      """
    Then the reply equals:
      """
      [1,1]
      """
    When node "master" executes:
      """
      ["CLIENT","ID"]
      """
    When I save the reply as "replicated-wait-id"
    When client "master" sends:
      """
      <command>
      """
    Then client "master" has no reply for 100 milliseconds
    When client "observer" on node "master" executes:
      """
      ["CLIENT","LIST","ID",{"saved":"replicated-wait-id"}]
      """
    Then the reply text contains "flags=b"
    When client "observer" on node "master" executes:
      """
      ["CLIENT","UNBLOCK",{"saved":"replicated-wait-id"},"TIMEOUT"]
      """
    Then the reply equals:
      """
      1
      """
    Then client "master" receives:
      """
      <reply>
      """
    Examples:
      | command            | reply |
      | ["WAIT",2,0]       | 1     |
      | ["WAITAOF",1,2,0]  | [1,1] |

  @command_CLIENT_PAUSE @command_PUBLISH @command_SPUBLISH
  Scenario Outline: WRITE pause prevents delivery until the publisher is admitted
    Given a fresh ffkv server
    When client "subscriber" sends:
      """
      ["<subscribe>","paused-news"]
      """
    Then client "subscriber" receives:
      """
      ["<ack>","paused-news",1]
      """
    When I execute:
      """
      ["CLIENT","PAUSE",10000,"WRITE"]
      """
    When client "publisher" sends:
      """
      ["<publish>","paused-news","held-message"]
      """
    Then client "publisher" has no reply for 50 milliseconds
    And client "subscriber" has no reply for 30 milliseconds
    When I execute:
      """
      ["CLIENT","UNPAUSE"]
      """
    Then client "publisher" receives:
      """
      1
      """
    Then client "subscriber" receives:
      """
      ["<message>","paused-news","held-message"]
      """
    Examples:
      | subscribe  | ack        | publish  | message  |
      | SUBSCRIBE  | subscribe  | PUBLISH  | message  |
      | SSUBSCRIBE | ssubscribe | SPUBLISH | smessage |

  @command_CLIENT_PAUSE @command_EXEC @command_PUBLISH
  Scenario: EXEC aggregates may-replicate pause classification from queued commands
    Given a fresh ffkv server
    When client "transaction" executes:
      """
      ["MULTI"]
      """
    When client "transaction" executes:
      """
      ["PUBLISH","news","message"]
      """
    Then the reply equals:
      """
      "QUEUED"
      """
    When I execute:
      """
      ["CLIENT","PAUSE",10000,"WRITE"]
      """
    When client "transaction" sends:
      """
      ["EXEC"]
      """
    Then client "transaction" has no reply for 50 milliseconds
    When I execute:
      """
      ["CLIENT","UNPAUSE"]
      """
    Then client "transaction" receives:
      """
      [0]
      """

  @command_SCRIPT_DEBUG @command_COPY @command_MOVE
  Scenario Outline: Disposable debugging restores an absent cross-database destination
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["SET","source","original"]]
      """
    When client "debugger" executes:
      """
      ["SCRIPT","DEBUG","YES"]
      """
    When client "debugger" sends:
      """
      ["EVAL","return redis.call('<operation>','source',<arguments>)",0]
      """
    Then client "debugger" receives:
      """
      ["* Stopped at 1","-> 1 return redis.call('<operation>','source',<arguments>)"]
      """
    When client "debugger" sends:
      """
      ["continue"]
      """
    Then client "debugger" receives:
      """
      ["<endsession>"]
      """
    Then client "debugger" receives:
      """
      1
      """
    When I execute:
      """
      ["GET","source"]
      """
    Then the reply equals:
      """
      "original"
      """
    When I execute:
      """
      ["SELECT",1]
      """
    When I execute:
      """
      ["GET","<destination>"]
      """
    Then the reply is null
    Examples:
      | operation | arguments  | destination |
      | COPY      | 'copy','DB',1 | copy      |
      | MOVE      | 1          | source      |

  @command_SCRIPT_DEBUG @command_MODULE_LOAD
  Scenario Outline: A disconnected debugger queued behind a native command cannot strand the actor
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["MODULE","LOAD","{{module_fixture}}"]]
      """
    When client "debugger" executes:
      """
      ["SCRIPT","DEBUG","YES"]
      """
    When client "holder" sends:
      """
      ["fixture.hold","{{directory}}/actor-held",800]
      """
    Then the file "{{directory}}/actor-held" exists
    When client "debugger" sends:
      """
      ["EVAL","<script>",0]
      """
    Then client "debugger" has no reply for 50 milliseconds
    When client "debugger" disconnects
    Then client "holder" receives:
      """
      "OK"
      """
    When I execute:
      """
      ["GET","abandoned"]
      """
    Then the reply is null
    When client "new-debugger" executes:
      """
      ["EVAL","return 42",0]
      """
    Then the reply equals:
      """
      42
      """
    Examples:
      | script                                                   |
      | return redis.call('SET','abandoned','must-not-exist')     |
      | while true do end                                       |

  @command_ACL_LOG
  Scenario Outline: ACL LOG records only the rejected command, key or channel
    Given a fresh ffkv server
    And client "audit" uses RESP 3
    And the following commands succeed:
      """
      [["ACL","SETUSER","restricted","on",">password-secret","+get","+publish","~allowed","&public"],["ACL","LOG","RESET"]]
      """
    When client "restricted" executes:
      """
      ["AUTH","restricted","password-secret"]
      """
    When client "restricted" executes:
      """
      <command>
      """
    Then the reply is an error with code "NOPERM"
    When client "audit" executes:
      """
      ["ACL","LOG"]
      """
    Then the reply has 1 items
    And the reply contains:
      """
      [{"reason":"<reason>","object":"<object>","context":"toplevel","username":"restricted","count":1}]
      """
    And the ACL log has client metadata without these secrets:
      """
      ["password-secret","payload-secret"]
      """
    Examples:
      | command                                    | reason  | object  |
      | ["SET","allowed","payload-secret"]         | command | set     |
      | ["GET","hidden"]                           | key     | hidden  |
      | ["PUBLISH","private","payload-secret"]      | channel | private |

  @command_ACL_LOG @command_EVAL
  Scenario: Nested Lua key denial retains the calling identity and Lua context
    Given a fresh ffkv server
    And client "audit" uses RESP 3
    And the following commands succeed:
      """
      [["ACL","SETUSER","script-user","on","nopass","+eval","+get","~allowed"]]
      """
    When client "script-user" executes:
      """
      ["AUTH","script-user","unused"]
      """
    When client "script-user" executes:
      """
      ["EVAL","return redis.call('GET','hidden')",0]
      """
    Then the reply is an error with code "NOPERM"
    When client "audit" executes:
      """
      ["ACL","LOG"]
      """
    Then the reply contains:
      """
      [{"reason":"key","object":"hidden","context":"lua","username":"script-user"}]
      """

  @command_ACL_LOG @command_EXEC
  Scenario: EXEC reauthorization logs denials in transaction context after an ACL update
    Given a fresh ffkv server
    And client "audit" uses RESP 3
    And the following commands succeed:
      """
      [["ACL","SETUSER","transaction-user","on","nopass","+get","+multi","+exec","~*"]]
      """
    When client "transaction-user" executes:
      """
      ["AUTH","transaction-user","unused"]
      """
    When client "transaction-user" executes:
      """
      ["MULTI"]
      """
    When client "transaction-user" executes:
      """
      ["GET","hidden"]
      """
    Then the reply equals:
      """
      "QUEUED"
      """
    When I execute:
      """
      ["ACL","SETUSER","transaction-user","resetkeys","~allowed"]
      """
    When client "transaction-user" executes:
      """
      ["EXEC"]
      """
    Then the EXEC reply contains an error with code "NOPERM"
    When client "audit" executes:
      """
      ["ACL","LOG"]
      """
    Then the reply contains:
      """
      [{"reason":"key","object":"hidden","context":"multi","username":"transaction-user"}]
      """

  @command_ACL_LOG @command_MODULE_LOAD
  Scenario: Native module callbacks cannot hide command denials from ACL LOG
    Given a fresh ffkv server
    And client "audit" uses RESP 3
    And the following commands succeed:
      """
      [["MODULE","LOAD","{{module_fixture}}"],["ACL","SETUSER","module-user","on","nopass","+fixture.get","~*"]]
      """
    When client "module-user" executes:
      """
      ["AUTH","module-user","unused"]
      """
    When client "module-user" executes:
      """
      ["fixture.get","hidden"]
      """
    Then the reply is an error with code "NOPERM"
    When client "audit" executes:
      """
      ["ACL","LOG"]
      """
    Then the reply contains:
      """
      [{"reason":"command","object":"get","context":"module","username":"module-user"}]
      """

  @command_ACL_LOG @command_FT_SEARCH @command_EVAL
  Scenario: Dynamic index namespace denials are logged without logging visibility filtering
    Given a fresh ffkv server
    And client "audit" uses RESP 3
    And the following commands succeed:
      """
      [["FT.CREATE","private-index","ON","HASH","PREFIX",1,"private:","SCHEMA","name","TEXT"],["ACL","SETUSER","search-user","on","nopass","+ft.search","+ft._list","+eval","~allowed:*"]]
      """
    When client "search-user" executes:
      """
      ["AUTH","search-user","unused"]
      """
    When client "search-user" executes:
      """
      ["FT._LIST"]
      """
    Then the reply equals:
      """
      []
      """
    When client "audit" executes:
      """
      ["ACL","LOG"]
      """
    Then the reply equals:
      """
      []
      """
    When client "search-user" executes:
      """
      ["FT.SEARCH","private-index","*"]
      """
    Then the reply is an error with code "NOPERM"
    When client "audit" executes:
      """
      ["ACL","LOG"]
      """
    Then the reply contains:
      """
      [{"reason":"key","object":"private:","context":"toplevel","username":"search-user"}]
      """
    When I execute:
      """
      ["ACL","LOG","RESET"]
      """
    When client "search-user" executes:
      """
      ["EVAL","return redis.call('FT.SEARCH','private-index','*')",0]
      """
    Then the reply is an error with code "NOPERM"
    When client "audit" executes:
      """
      ["ACL","LOG"]
      """
    Then the reply contains:
      """
      [{"reason":"key","object":"private:","context":"lua","username":"search-user"}]
      """

  @command_ACL_LOG @command_FT_DROPINDEX
  Scenario: Dynamic document deletion denials identify only the rejected document
    Given a fresh ffkv server
    And client "audit" uses RESP 3
    And the following commands succeed:
      """
      [["FT.CREATE","documents","ON","HASH","PREFIX",1,"doc:","SCHEMA","name","TEXT"],["HSET","doc:one","name","document-secret"],["ACL","SETUSER","drop-user","on","nopass","+ft.dropindex","%R~doc:*","%W~allowed:*"]]
      """
    When client "drop-user" executes:
      """
      ["AUTH","drop-user","unused"]
      """
    When client "drop-user" executes:
      """
      ["FT.DROPINDEX","documents","DD"]
      """
    Then the reply is an error with code "NOPERM"
    When client "audit" executes:
      """
      ["ACL","LOG"]
      """
    Then the reply contains:
      """
      [{"reason":"key","object":"doc:one","context":"toplevel","username":"drop-user"}]
      """
    And the ACL log has client metadata without these secrets:
      """
      ["document-secret"]
      """
    When I execute:
      """
      ["HGET","doc:one","name"]
      """
    Then the reply equals:
      """
      "document-secret"
      """

  @command_SORT_RO @command_CLIENT_TRACKING
  Scenario: SORT_RO tracks real scalar and hash lookups rather than wildcard patterns
    Given a fresh ffkv server
    And client "sort-cache" uses RESP 3
    And the following commands succeed:
      """
      [["RPUSH","sort-ids","2","1"],["SET","sort-score:1",10],["SET","sort-score:2",20],["HSET","sort-object:1","name","Alice"],["HSET","sort-object:2","name","Bob"]]
      """
    When client "sort-cache" executes:
      """
      ["CLIENT","TRACKING","ON"]
      """
    When client "sort-cache" executes:
      """
      ["SORT_RO","sort-ids","BY","sort-score:*","GET","sort-object:*->name"]
      """
    Then the reply equals:
      """
      ["Alice","Bob"]
      """
    When I execute:
      """
      ["SET","sort-score:unused",1]
      """
    Then client "sort-cache" has no reply for 30 milliseconds
    When I execute:
      """
      ["HSET","sort-object:1","name","Alicia"]
      """
    Then client "sort-cache" receives:
      """
      {"type":"push","values":["invalidate",["sort-object:1"]]}
      """
    When I execute:
      """
      ["SET","sort-score:2",5]
      """
    Then client "sort-cache" receives:
      """
      {"type":"push","values":["invalidate",["sort-score:2"]]}
      """
    When client "sort-cache" executes:
      """
      ["SORT_RO","sort-ids","BY","sort-score:*","GET","sort-object:*->name"]
      """
    Then the reply equals:
      """
      ["Bob","Alicia"]
      """

  @command_SORT @command_ACL_LOG
  Scenario: A denied actual SORT lookup logs the resource and leaves STORE unchanged
    Given a fresh ffkv server
    And client "audit" uses RESP 3
    And the following commands succeed:
      """
      [["RPUSH","sort-source","1"],["SET","secret:1","sort-value-secret"],["SET","sort-destination","unchanged"],["ACL","SETUSER","sort-user","on","nopass","+sort","%R~sort-source","%W~sort-destination"]]
      """
    When client "sort-user" executes:
      """
      ["AUTH","sort-user","unused"]
      """
    When client "sort-user" executes:
      """
      ["SORT","sort-source","BY","secret:*","ALPHA","STORE","sort-destination"]
      """
    Then the reply is an error with code "NOPERM"
    When client "audit" executes:
      """
      ["ACL","LOG"]
      """
    Then the reply contains:
      """
      [{"reason":"key","object":"secret:1","context":"toplevel","username":"sort-user"}]
      """
    And the ACL log has client metadata without these secrets:
      """
      ["sort-value-secret"]
      """
    When I execute:
      """
      ["GET","sort-destination"]
      """
    Then the reply equals:
      """
      "unchanged"
      """
