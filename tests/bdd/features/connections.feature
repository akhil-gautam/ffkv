Feature: Live connection identity, control, and coherent client caches

  @command_AUTH
  Scenario: Authentication changes the current user on a persistent connection
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["ACL","SETUSER","app","on",">secret","~*","+@all"]]
      """
    When client "application" executes:
      """
      ["AUTH","app","secret"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "application" executes:
      """
      ["ACL","WHOAMI"]
      """
    Then the reply equals:
      """
      "app"
      """

  @command_AUTH @command_ACL_SETUSER @command_ACL_WHOAMI
  Scenario: Password-only authentication requires a default password without changing the prior identity on failure
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["ACL","SETUSER","app","on",">secret","~*","+@all"]]
      """
    When client "application" executes:
      """
      ["AUTH","app","secret"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "application" executes:
      """
      ["AUTH","arbitrary"]
      """
    Then the reply is an error with code "ERR"
    When client "application" executes:
      """
      ["ACL","WHOAMI"]
      """
    Then the reply equals:
      """
      "app"
      """
    When client "application" executes:
      """
      ["AUTH","default","arbitrary"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "application" executes:
      """
      ["ACL","WHOAMI"]
      """
    Then the reply equals:
      """
      "default"
      """
    And the following commands succeed:
      """
      [["ACL","SETUSER","default",">configured"]]
      """
    When client "application" executes:
      """
      ["AUTH","configured"]
      """
    Then the reply equals:
      """
      "OK"
      """

  @command_HELLO @command_CLIENT_SETNAME @command_CLIENT_GETNAME @command_CLIENT_ID
  Scenario: HELLO negotiates RESP3 while preserving identity and setting a connection-local name
    Given a fresh ffkv server
    When client "named" executes:
      """
      ["CLIENT","ID"]
      """
    Then the reply is a positive integer
    When I save the reply as "identity"
    When client "named" executes:
      """
      ["HELLO","3","SETNAME","api-worker"]
      """
    Then the reply contains:
      """
      {"proto":3,"role":"master"}
      """
    When client "named" executes:
      """
      ["CLIENT","ID"]
      """
    Then the reply equals the saved value "identity"
    When client "named" executes:
      """
      ["CLIENT","GETNAME"]
      """
    Then the reply equals:
      """
      "api-worker"
      """
    When client "other" executes:
      """
      ["CLIENT","GETNAME"]
      """
    Then the reply is null
    When client "named" executes:
      """
      ["CLIENT","SETNAME","renamed"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "named" executes:
      """
      ["CLIENT","GETNAME"]
      """
    Then the reply equals:
      """
      "renamed"
      """

  @command_CLIENT_INFO @command_CLIENT_LIST @command_CLIENT_SETINFO @command_SELECT
  Scenario: Client inspection reflects real metadata and the selected database with scoped ID filtering
    Given a fresh ffkv server
    When client "named" executes:
      """
      ["CLIENT","SETNAME","inspected"]
      """
    When client "named" executes:
      """
      ["CLIENT","SETINFO","LIB-NAME","native-bdd"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "named" executes:
      """
      ["CLIENT","SETINFO","LIB-VER","8.10"]
      """
    When client "named" executes:
      """
      ["SELECT","2"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "named" executes:
      """
      ["CLIENT","INFO"]
      """
    Then the reply text contains "name=inspected"
    And the reply text contains "db=2"
    And the reply text contains "cmd=client"
    And the reply text contains "lib-name=native-bdd"
    And the reply text contains "lib-ver=8.10"
    And the reply text contains "addr=127.0.0.1:"
    And the reply text contains "user=default"
    When client "named" executes:
      """
      ["CLIENT","ID"]
      """
    When I save the reply as "inspected-id"
    When I execute:
      """
      ["CLIENT","LIST","TYPE","NORMAL","ID",{"saved":"inspected-id"}]
      """
    Then the reply text contains "name=inspected"
    And the reply has 1 client records
    When I execute:
      """
      ["CLIENT","LIST","TYPE","PUBSUB","ID",{"saved":"inspected-id"}]
      """
    Then the reply equals:
      """
      ""
      """

  @command_SELECT
  Scenario: Database selection persists per connection without leaking writes to another database
    Given a fresh ffkv server
    When client "one" executes:
      """
      ["SELECT","1"]
      """
    When client "one" executes:
      """
      ["SET","same-key","database-one"]
      """
    When I execute:
      """
      ["SET","same-key","database-zero"]
      """
    When client "one" executes:
      """
      ["GET","same-key"]
      """
    Then the reply equals:
      """
      "database-one"
      """
    When I execute:
      """
      ["GET","same-key"]
      """
    Then the reply equals:
      """
      "database-zero"
      """

  @command_CLIENT_NO_EVICT @command_CLIENT_NO_TOUCH
  Scenario: Eviction and key-touch policies are connection-local and visible in client flags
    Given a fresh ffkv server
    When client "protected" executes:
      """
      ["CLIENT","NO-EVICT","ON"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "protected" executes:
      """
      ["CLIENT","INFO"]
      """
    Then the reply text contains "flags=e"
    When client "protected" executes:
      """
      ["CLIENT","NO-EVICT","OFF"]
      """
    When client "protected" executes:
      """
      ["CLIENT","NO-TOUCH","ON"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "protected" executes:
      """
      ["CLIENT","INFO"]
      """
    Then the reply text contains "flags=T"
    When client "other" executes:
      """
      ["CLIENT","INFO"]
      """
    Then the reply text contains "flags=N"

  @command_CLIENT_KILL
  Scenario: Killing a selected client closes its actual socket without killing another connection
    Given a fresh ffkv server
    When client "victim" executes:
      """
      ["CLIENT","ID"]
      """
    When I save the reply as "victim-id"
    When client "survivor" executes:
      """
      ["PING"]
      """
    When I execute:
      """
      ["CLIENT","KILL","ID",{"saved":"victim-id"},"SKIPME","YES"]
      """
    Then the reply equals:
      """
      1
      """
    Then client "victim" is disconnected
    When client "survivor" executes:
      """
      ["PING"]
      """
    Then the reply equals:
      """
      "PONG"
      """
    When I execute:
      """
      ["CLIENT","LIST","ID",{"saved":"victim-id"}]
      """
    Then the reply equals:
      """
      ""
      """

  @command_CLIENT_PAUSE @command_CLIENT_UNPAUSE
  Scenario: WRITE pause delays a real mutation but permits reads and an explicit unpause
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["SET","paused-key","before"]]
      """
    When I execute:
      """
      ["CLIENT","PAUSE","10000","WRITE"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "writer" sends:
      """
      ["SET","paused-key","after"]
      """
    Then client "writer" has no reply for 50 milliseconds
    When I execute:
      """
      ["GET","paused-key"]
      """
    Then the reply equals:
      """
      "before"
      """
    When I execute:
      """
      ["CLIENT","UNPAUSE"]
      """
    Then the reply equals:
      """
      "OK"
      """
    Then client "writer" receives:
      """
      "OK"
      """
    When I execute:
      """
      ["GET","paused-key"]
      """
    Then the reply equals:
      """
      "after"
      """

  @command_CLIENT_UNBLOCK
  Scenario: Unblock completes a real list waiter with timeout semantics and does not consume later data
    Given a fresh ffkv server
    When client "waiting" executes:
      """
      ["CLIENT","ID"]
      """
    When I save the reply as "waiting-id"
    When client "waiting" sends:
      """
      ["BLPOP","empty-list","0"]
      """
    Then client "waiting" is blocked
    When I execute:
      """
      ["CLIENT","UNBLOCK",{"saved":"waiting-id"},"TIMEOUT"]
      """
    Then the reply equals:
      """
      1
      """
    Then client "waiting" receives:
      """
      null
      """
    When I execute:
      """
      ["CLIENT","UNBLOCK",{"saved":"waiting-id"}]
      """
    Then the reply equals:
      """
      0
      """
    When I execute:
      """
      ["RPUSH","empty-list","retained"]
      """
    When client "waiting" executes:
      """
      ["LPOP","empty-list"]
      """
    Then the reply equals:
      """
      "retained"
      """

  @command_CLIENT_REPLY
  Scenario: OFF suppresses wire replies while executing commands and SKIP affects exactly the next reply
    Given a fresh ffkv server
    When client "silent" sends:
      """
      ["CLIENT","REPLY","OFF"]
      """
    When client "silent" sends:
      """
      ["SET","silent-write","performed"]
      """
    Then client "silent" has no reply for 50 milliseconds
    When client "silent" sends:
      """
      ["CLIENT","REPLY","ON"]
      """
    Then client "silent" receives:
      """
      "OK"
      """
    When I execute:
      """
      ["GET","silent-write"]
      """
    Then the reply equals:
      """
      "performed"
      """
    When client "silent" sends:
      """
      ["CLIENT","REPLY","SKIP"]
      """
    When client "silent" sends:
      """
      ["ECHO","must-not-arrive"]
      """
    Then client "silent" has no reply for 50 milliseconds
    When client "silent" sends:
      """
      ["ECHO","arrives"]
      """
    Then client "silent" receives:
      """
      "arrives"
      """

  @command_CLIENT_TRACKING
  Scenario: RESP3 tracking invalidates stale cached values and subsequent reads see new values
    Given a fresh ffkv server
    And client "cache" uses RESP 3
    And the following commands succeed:
      """
      [["SET","cached","old"]]
      """
    When client "cache" executes:
      """
      ["CLIENT","TRACKING","ON"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "cache" executes:
      """
      ["GET","cached"]
      """
    Then the reply equals:
      """
      "old"
      """
    When I execute:
      """
      ["SET","cached","new"]
      """
    Then client "cache" receives:
      """
      {"type":"push","values":["invalidate",["cached"]]}
      """
    When client "cache" executes:
      """
      ["GET","cached"]
      """
    Then the reply equals:
      """
      "new"
      """
    When client "cache" executes:
      """
      ["CLIENT","TRACKING","OFF"]
      """
    When I execute:
      """
      ["SET","cached","untracked"]
      """
    Then client "cache" has no reply for 50 milliseconds

  @command_CLIENT_GETREDIR @command_CLIENT_TRACKING
  Scenario: RESP2 redirects invalidations only to the requested persistent subscriber
    Given a fresh ffkv server
    When client "invalidations" executes:
      """
      ["CLIENT","ID"]
      """
    When I save the reply as "redirect-id"
    When client "invalidations" sends:
      """
      ["SUBSCRIBE","__redis__:invalidate"]
      """
    Then client "invalidations" receives:
      """
      ["subscribe","__redis__:invalidate",1]
      """
    When client "cache" executes:
      """
      ["CLIENT","TRACKING","ON","REDIRECT",{"saved":"redirect-id"}]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "cache" executes:
      """
      ["CLIENT","GETREDIR"]
      """
    Then the reply equals the saved value "redirect-id"
    When client "cache" executes:
      """
      ["GET","missing-cache-key"]
      """
    Then the reply is null
    When I execute:
      """
      ["SET","missing-cache-key","now-present"]
      """
    Then client "invalidations" receives:
      """
      ["message","__redis__:invalidate",["missing-cache-key"]]
      """
    When client "cache" executes:
      """
      ["CLIENT","TRACKING","OFF"]
      """
    When client "cache" executes:
      """
      ["CLIENT","GETREDIR"]
      """
    Then the reply equals:
      """
      -1
      """

  @command_CLIENT_CACHING @command_CLIENT_TRACKINGINFO
  Scenario: OPTIN tracks only the next opted-in read and reports the active tracking flags
    Given a fresh ffkv server
    And client "cache" uses RESP 3
    When client "cache" executes:
      """
      ["CLIENT","TRACKING","ON","OPTIN"]
      """
    When client "cache" executes:
      """
      ["CLIENT","TRACKINGINFO"]
      """
    Then the reply equals:
      """
      {"flags":["on","optin"],"redirect":0,"prefixes":[]}
      """
    When client "cache" executes:
      """
      ["GET","not-opted-in"]
      """
    When I execute:
      """
      ["SET","not-opted-in","ignored"]
      """
    Then client "cache" has no reply for 50 milliseconds
    When client "cache" executes:
      """
      ["CLIENT","CACHING","YES"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "cache" executes:
      """
      ["GET","opted-in"]
      """
    When I execute:
      """
      ["SET","opted-in","observed"]
      """
    Then client "cache" receives:
      """
      {"type":"push","values":["invalidate",["opted-in"]]}
      """
    When client "cache" executes:
      """
      ["GET","next-read"]
      """
    When I execute:
      """
      ["SET","next-read","not-sticky"]
      """
    Then client "cache" has no reply for 50 milliseconds

  @command_CLIENT_TRACKING @command_CLIENT_CACHING
  Scenario: OPTOUT can skip a read and NOLOOP suppresses only the originating writer
    Given a fresh ffkv server
    And client "cache" uses RESP 3
    When client "cache" executes:
      """
      ["CLIENT","TRACKING","ON","OPTOUT","NOLOOP"]
      """
    When client "cache" executes:
      """
      ["CLIENT","CACHING","NO"]
      """
    When client "cache" executes:
      """
      ["GET","excluded"]
      """
    When I execute:
      """
      ["SET","excluded","not-cached"]
      """
    Then client "cache" has no reply for 50 milliseconds
    When client "cache" executes:
      """
      ["GET","loop-key"]
      """
    When client "cache" executes:
      """
      ["SET","loop-key","self-written"]
      """
    Then the reply equals:
      """
      "OK"
      """
    Then client "cache" has no reply for 50 milliseconds
    When client "cache" executes:
      """
      ["GET","loop-key"]
      """
    Then the reply equals:
      """
      "self-written"
      """
    When I execute:
      """
      ["SET","loop-key","other-written"]
      """
    Then client "cache" receives:
      """
      {"type":"push","values":["invalidate",["loop-key"]]}
      """

  @command_CLIENT_TRACKING
  Scenario: Broadcast prefixes filter writes without requiring reads and flush clears the entire cache
    Given a fresh ffkv server
    And client "cache" uses RESP 3
    When client "cache" executes:
      """
      ["CLIENT","TRACKING","ON","BCAST","PREFIX","user:"]
      """
    When I execute:
      """
      ["SET","order:1","ignored"]
      """
    Then client "cache" has no reply for 50 milliseconds
    When I execute:
      """
      ["SET","user:1","invalidate-without-read"]
      """
    Then client "cache" receives:
      """
      {"type":"push","values":["invalidate",["user:1"]]}
      """
    When I execute:
      """
      ["FLUSHDB"]
      """
    Then client "cache" receives:
      """
      {"type":"push","values":["invalidate",null]}
      """

  @command_RESET
  Scenario: RESET clears subscriptions, database, names, protocol and tracking without closing the connection
    Given a fresh ffkv server
    And client "resettable" uses RESP 3
    When client "resettable" executes:
      """
      ["CLIENT","SETNAME","temporary"]
      """
    When client "resettable" executes:
      """
      ["SELECT","3"]
      """
    When client "resettable" executes:
      """
      ["CLIENT","TRACKING","ON","BCAST"]
      """
    When client "resettable" sends:
      """
      ["SUBSCRIBE","old-channel"]
      """
    Then client "resettable" receives:
      """
      {"type":"push","values":["subscribe","old-channel",1]}
      """
    When client "resettable" executes:
      """
      ["RESET"]
      """
    Then the reply equals:
      """
      "RESET"
      """
    When client "resettable" executes:
      """
      ["CLIENT","GETNAME"]
      """
    Then the reply is null
    When client "resettable" executes:
      """
      ["CLIENT","GETREDIR"]
      """
    Then the reply equals:
      """
      -1
      """
    When client "resettable" executes:
      """
      ["CLIENT","INFO"]
      """
    Then the reply text contains "db=0"
    And the reply text contains "resp=2"
    When I execute:
      """
      ["PUBLISH","old-channel","not-delivered"]
      """
    Then the reply equals:
      """
      0
      """
    When I execute:
      """
      ["SET","after-reset","not-tracked"]
      """
    Then client "resettable" has no reply for 50 milliseconds

  @command_ECHO @command_PING @command_QUIT
  Scenario: Binary echo and ping preserve payloads and QUIT closes the persistent socket
    Given a fresh ffkv server
    When client "closing" executes:
      """
      ["ECHO",{"base64":"AP8NCg=="}]
      """
    Then the reply equals:
      """
      {"type":"bytes","base64":"AP8NCg=="}
      """
    When client "closing" executes:
      """
      ["PING","alive"]
      """
    Then the reply equals:
      """
      "alive"
      """
    When client "closing" executes:
      """
      ["QUIT"]
      """
    Then the reply equals:
      """
      "OK"
      """
    Then client "closing" is disconnected

  @command_CLIENT_UNBLOCK
  Scenario: Error unblocking removes the actual waiter and exposes the UNBLOCKED error
    Given a fresh ffkv server
    When client "waiting" executes:
      """
      ["CLIENT","ID"]
      """
    When I save the reply as "waiting-id"
    When client "waiting" sends:
      """
      ["BLPOP","error-list","0"]
      """
    Then client "waiting" is blocked
    When I execute:
      """
      ["CLIENT","UNBLOCK",{"saved":"waiting-id"},"ERROR"]
      """
    Then the reply equals:
      """
      1
      """
    Then client "waiting" receives an error with code "UNBLOCKED"
    When client "waiting" executes:
      """
      ["PING"]
      """
    Then the reply equals:
      """
      "PONG"
      """

  @command_CLIENT_TRACKING
  Scenario: Expiration invalidates tracked values instead of leaving an expired cache entry
    Given a fresh ffkv server
    And client "cache" uses RESP 3
    And the following commands succeed:
      """
      [["SET","expiring-cache","old"]]
      """
    When client "cache" executes:
      """
      ["CLIENT","TRACKING","ON"]
      """
    When client "cache" executes:
      """
      ["GET","expiring-cache"]
      """
    Then the reply equals:
      """
      "old"
      """
    When I execute:
      """
      ["PEXPIREAT","expiring-cache","1"]
      """
    Then the reply equals:
      """
      1
      """
    Then client "cache" receives:
      """
      {"type":"push","values":["invalidate",["expiring-cache"]]}
      """
    When client "cache" executes:
      """
      ["GET","expiring-cache"]
      """
    Then the reply is null

  @command_CLIENT_TRACKING @command_SELECT
  Scenario: Tracking shares Redis key names across databases and FLUSHDB invalidates cached results
    Given a fresh ffkv server
    And client "cache" uses RESP 3
    When client "cache" executes:
      """
      ["SELECT","2"]
      """
    When client "cache" executes:
      """
      ["SET","same-cache-name","database-two"]
      """
    When client "cache" executes:
      """
      ["CLIENT","TRACKING","ON"]
      """
    When client "cache" executes:
      """
      ["GET","same-cache-name"]
      """
    When I execute:
      """
      ["SET","same-cache-name","database-zero"]
      """
    Then client "cache" receives:
      """
      {"type":"push","values":["invalidate",["same-cache-name"]]}
      """
    When client "cache" executes:
      """
      ["GET","same-cache-name"]
      """
    Then the reply equals:
      """
      "database-two"
      """
    When I execute:
      """
      ["FLUSHDB"]
      """
    Then client "cache" receives:
      """
      {"type":"push","values":["invalidate",null]}
      """

  @command_CLIENT_KILL
  Scenario: Killing the issuing connection replies with the count before closing its socket
    Given a fresh ffkv server
    When client "self-kill" executes:
      """
      ["CLIENT","ID"]
      """
    When I save the reply as "self-id"
    When client "self-kill" executes:
      """
      ["CLIENT","KILL","ID",{"saved":"self-id"},"SKIPME","NO"]
      """
    Then the reply equals:
      """
      1
      """
    Then client "self-kill" is disconnected

  @command_CLIENT_CACHING @command_CLIENT_TRACKING
  Scenario: Opting in before MULTI tracks all reads in that transaction
    Given a fresh ffkv server
    And client "cache" uses RESP 3
    When client "cache" executes:
      """
      ["CLIENT","TRACKING","ON","OPTIN"]
      """
    When client "cache" executes:
      """
      ["CLIENT","CACHING","YES"]
      """
    When client "cache" executes:
      """
      ["MULTI"]
      """
    When client "cache" executes:
      """
      ["GET","transaction-cache"]
      """
    Then the reply equals:
      """
      "QUEUED"
      """
    When client "cache" executes:
      """
      ["EXEC"]
      """
    Then the reply equals:
      """
      [null]
      """
    When I execute:
      """
      ["SET","transaction-cache","invalidate"]
      """
    Then client "cache" receives:
      """
      {"type":"push","values":["invalidate",["transaction-cache"]]}
      """
