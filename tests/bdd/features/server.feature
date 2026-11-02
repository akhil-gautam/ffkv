Feature: Stateful server administration and diagnostics
  Administrative replies describe real runtime state, persistence, and security.

  @command_CONFIG_GET @command_CONFIG_SET
  Scenario: Configuration updates validate the complete transaction before publication
    Given a fresh ffkv server
    And client "default" uses RESP 3
    When I execute:
      """
      ["CONFIG","SET","latency-monitor-threshold","7","slowlog-max-len","9"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["CONFIG","GET","latency-monitor-*","slowlog-max-len"]
      """
    Then the reply contains:
      """
      {"latency-monitor-threshold":"7","slowlog-max-len":"9"}
      """
    When I execute:
      """
      ["CONFIG","SET","latency-monitor-threshold","8","slowlog-max-len","-1"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["CONFIG","GET","latency-monitor-threshold"]
      """
    Then the reply contains:
      """
      {"latency-monitor-threshold":"7"}
      """

  @command_CONFIG_REWRITE
  Scenario: Rewritten effective configuration is loaded after a crash
    Given a fresh ffkv server
    And client "default" uses RESP 3
    And the following commands succeed:
      """
      [["CONFIG","SET","slowlog-max-len","37"],["CONFIG","REWRITE"]]
      """
    When ffkv restarts after a crash
    When I execute:
      """
      ["CONFIG","GET","slowlog-max-len"]
      """
    Then the reply contains:
      """
      {"slowlog-max-len":"37"}
      """

  @command_CONFIG_RESETSTAT @command_LATENCY_HISTOGRAM
  Scenario: Histogram calls describe actual commands and reset independently of dataset
    Given a fresh ffkv server
    And client "default" uses RESP 3
    And the following commands succeed:
      """
      [["CONFIG","RESETSTAT"],["SET","hist:key","value"],["GET","hist:key"]]
      """
    When I execute:
      """
      ["LATENCY","HISTOGRAM","get"]
      """
    Then the reply has JSON path "get.calls" equal to:
      """
      1
      """
    When I execute:
      """
      ["CONFIG","RESETSTAT"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["LATENCY","HISTOGRAM","get"]
      """
    Then the reply equals:
      """
      {}
      """
    When I execute:
      """
      ["GET","hist:key"]
      """
    Then the reply equals:
      """
      "value"
      """

  @command_SAVE @command_LASTSAVE
  Scenario: A synchronous save creates a recoverable current dataset
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["SET","save:key","saved"],["SAVE"]]
      """
    When I execute:
      """
      ["LASTSAVE"]
      """
    Then the reply is a positive integer
    When ffkv restarts after a crash
    When I execute:
      """
      ["GET","save:key"]
      """
    Then the reply equals:
      """
      "saved"
      """

  @command_BGSAVE @command_INFO
  Scenario: Background save completes with real persistence status
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["SET","background:key","saved"],["BGSAVE","SCHEDULE"]]
      """
    When I wait until executing:
      """
      {"command":["INFO","persistence"],"text_contains":"rdb_bgsave_in_progress:0"}
      """
    Then the reply text contains "rdb_last_bgsave_status:ok"
    When ffkv restarts after a crash
    When I execute:
      """
      ["GET","background:key"]
      """
    Then the reply equals:
      """
      "saved"
      """

  @command_BGREWRITEAOF
  Scenario: Rewritten journal preserves live values and deletes after restart
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["SET","rewrite:keep","one"],["SET","rewrite:gone","two"],["DEL","rewrite:gone"],["BGREWRITEAOF"]]
      """
    When I wait until executing:
      """
      {"command":["INFO","persistence"],"text_contains":"aof_rewrite_in_progress:0"}
      """
    Then the reply text contains "aof_last_bgrewrite_status:ok"
    When ffkv restarts after a crash
    When I execute:
      """
      ["MGET","rewrite:keep","rewrite:gone"]
      """
    Then the reply equals:
      """
      ["one",null]
      """

  @command_BACKUP_START @command_BACKUP_STATUS @command_BACKUP_LIST @command_BACKUP_SEAL
  Scenario: Sealed backup restores the seal boundary including incremental writes
    Given a fresh ffkv server
    And client "default" uses RESP 3
    And the following commands succeed:
      """
      [["SET","backup:base","base"],["BACKUP","START"]]
      """
    When I wait until executing:
      """
      {"command":["BACKUP","STATUS"],"contains":{"state":"incrementing"}}
      """
    And the following commands succeed:
      """
      [["SET","backup:increment","increment"],["BACKUP","SEAL"]]
      """
    When I execute:
      """
      ["BACKUP","STATUS"]
      """
    Then the reply contains:
      """
      {"state":"sealed","error":""}
      """
    When I execute:
      """
      ["BACKUP","LIST"]
      """
    When the following commands succeed:
      """
      [["SET","backup:after-seal","must-not-restore"]]
      """
    When ffkv restarts from the sealed backup
    When I execute:
      """
      ["MGET","backup:base","backup:increment","backup:after-seal"]
      """
    Then the reply equals:
      """
      ["base","increment",null]
      """

  @command_BACKUP_ABORT @command_BACKUP_CLEANUP
  Scenario: Aborting an unsealed backup releases it for a new complete lifecycle
    Given a fresh ffkv server
    And client "default" uses RESP 3
    And the following commands succeed:
      """
      [["SET","backup:key","value"],["BACKUP","START"]]
      """
    When I wait until executing:
      """
      {"command":["BACKUP","STATUS"],"contains":{"state":"incrementing"}}
      """
    When I execute:
      """
      ["BACKUP","ABORT"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["BACKUP","STATUS"]
      """
    Then the reply contains:
      """
      {"state":"failed"}
      """
    And the following commands succeed:
      """
      [["BACKUP","CLEANUP"],["BACKUP","START"]]
      """
    When I wait until executing:
      """
      {"command":["BACKUP","STATUS"],"contains":{"state":"incrementing"}}
      """
    And the following commands succeed:
      """
      [["BACKUP","SEAL"],["BACKUP","CLEANUP"]]
      """
    When I execute:
      """
      ["BACKUP","STATUS"]
      """
    Then the reply contains:
      """
      {"state":"idle"}
      """
    When I execute:
      """
      ["BACKUP","LIST"]
      """
    Then the reply equals:
      """
      []
      """

  @command_HOTKEYS @command_HOTKEYS_START @command_HOTKEYS_GET @command_HOTKEYS_STOP @command_HOTKEYS_RESET
  Scenario: Hotkey network tracking measures requests and preserves stopped results until reset
    Given a fresh ffkv server
    And client "telemetry" uses RESP 3
    And the following commands succeed:
      """
      [["SET","hot:item","payload"],["HOTKEYS","START","METRICS","1","NET","COUNT","1","SAMPLE","1"],["GET","hot:item"],["GET","hot:item"]]
      """
    When client "telemetry" executes:
      """
      ["HOTKEYS","GET"]
      """
    Then the reply has JSON path "0.tracking-active" equal to:
      """
      1
      """
    And the reply has JSON path "0.by-net-bytes.0" equal to:
      """
      "hot:item"
      """
    When I execute:
      """
      ["HOTKEYS","STOP"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "telemetry" executes:
      """
      ["HOTKEYS","GET"]
      """
    Then the reply has JSON path "0.tracking-active" equal to:
      """
      0
      """
    When I execute:
      """
      ["HOTKEYS","RESET"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["HOTKEYS","GET"]
      """
    Then the reply is null

  @command_MEMORY_USAGE @command_MEMORY_STATS @command_MEMORY_DOCTOR @command_MEMORY_MALLOC_STATS @command_MEMORY_PURGE
  Scenario: Memory diagnostics account for live data without inventing allocator internals
    Given a fresh ffkv server
    And client "default" uses RESP 3
    And the following commands succeed:
      """
      [["SET","memory:item","a genuine allocated value"]]
      """
    When I execute:
      """
      ["MEMORY","USAGE","memory:item","SAMPLES","0"]
      """
    Then the reply is a positive integer
    When I execute:
      """
      ["MEMORY","STATS"]
      """
    Then the reply has JSON path "keys.count" equal to:
      """
      1
      """
    When I execute:
      """
      ["MEMORY","DOCTOR"]
      """
    Then the reply text contains "memory"
    When I execute:
      """
      ["MEMORY","MALLOC-STATS"]
      """
    Then the reply text contains "allocator"
    When I execute:
      """
      ["MEMORY","PURGE"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["GET","memory:item"]
      """
    Then the reply equals:
      """
      "a genuine allocated value"
      """
    And the following commands succeed:
      """
      [["DEL","memory:item"]]
      """
    When I execute:
      """
      ["MEMORY","USAGE","memory:item"]
      """
    Then the reply is null

  @command_LATENCY_HISTORY @command_LATENCY_LATEST @command_LATENCY_GRAPH @command_LATENCY_DOCTOR @command_LATENCY_RESET
  Scenario: Latency history and diagnostics reflect a recorded execution delay
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["MODULE","LOAD","{{module_fixture}}"],["CONFIG","SET","latency-monitor-threshold","1"],["LATENCY","RESET"],["fixture.hold","{{directory}}/latency-held",20]]
      """
    When I execute:
      """
      ["LATENCY","HISTORY","command"]
      """
    Then latency history contains a sample of at least 10 milliseconds
    When I execute:
      """
      ["LATENCY","LATEST"]
      """
    Then latest latency for "command" is at least 10 milliseconds
    When I execute:
      """
      ["LATENCY","GRAPH","command"]
      """
    When I execute:
      """
      ["LATENCY","DOCTOR"]
      """
    When I execute:
      """
      ["LATENCY","RESET","command"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["LATENCY","HISTORY","command"]
      """
    Then the reply equals:
      """
      []
      """

  @command_SLOWLOG_GET @command_SLOWLOG_LEN @command_SLOWLOG_RESET
  Scenario: Slow log contains actual command arguments and reset removes them
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["CONFIG","SET","slowlog-log-slower-than","0"],["SET","slow:actual","value"],["CONFIG","SET","slowlog-log-slower-than","-1"]]
      """
    When I execute:
      """
      ["SLOWLOG","LEN"]
      """
    Then the reply is a positive integer
    When I execute:
      """
      ["SLOWLOG","GET","-1"]
      """
    Then the slow log contains the executed command:
      """
      ["SET","slow:actual","value"]
      """
    When I execute:
      """
      ["SLOWLOG","RESET"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["SLOWLOG","LEN"]
      """
    Then the reply equals:
      """
      0
      """

  @command_MONITOR
  Scenario: Monitor emits actual failed requests but redacts credential arguments
    Given a fresh ffkv server
    When client "observer" executes:
      """
      ["MONITOR"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "caller" executes:
      """
      ["AUTH","missing-user","never-log-this-password"]
      """
    Then the reply is an error with code "WRONGPASS"
    Then client "observer" receives text containing "AUTH"
    And the received text does not contain "never-log-this-password"
    When I execute:
      """
      ["SET","monitor:actual","visible"]
      """
    Then the reply equals:
      """
      "OK"
      """
    Then client "observer" receives text containing "monitor:actual"

  @command_ROLE
  Scenario: Standalone role is an actual master without fabricated replicas
    Given a fresh ffkv server
    When I execute:
      """
      ["ROLE"]
      """
    Then the reply has JSON path "0" equal to:
      """
      "master"
      """
    And the reply has JSON path "2" equal to:
      """
      []
      """

  @command_TIME
  Scenario: Server time reports Unix seconds and a microsecond component
    Given a fresh ffkv server
    When I execute:
      """
      ["TIME"]
      """
    Then the reply is a Redis timestamp pair

  @command_LOLWUT
  Scenario: Diagnostic art identifies the actual server implementation
    Given a fresh ffkv server
    When I execute:
      """
      ["LOLWUT","VERSION","6"]
      """
    Then the reply text contains "ffkv"

  @command_SHUTDOWN
  Scenario: Saved shutdown exits and persists the dataset
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["SET","shutdown:actual","persisted"]]
      """
    When ffkv shuts down with SAVE
    When ffkv starts again
    When I execute:
      """
      ["GET","shutdown:actual"]
      """
    Then the reply equals:
      """
      "persisted"
      """

  @command_MODULE_LIST
  Scenario: Only actually loaded native libraries appear in the module list
    Given a fresh ffkv server
    When I execute:
      """
      ["MODULE","LIST"]
      """
    Then the reply equals:
      """
      []
      """
    Given the following commands succeed:
      """
      [["MODULE","LOAD","{{module_fixture}}","first"]]
      """
    Given client "metadata" uses RESP 3
    When client "metadata" executes:
      """
      ["MODULE","LIST"]
      """
    Then the reply contains:
      """
      [{"name":"fixture","ver":10000,"path":"{{module_fixture}}","args":["first"]}]
      """

  @command_MODULE_LOAD @command_MODULE_UNLOAD
  Scenario: Real native callbacks register commands and unload releases module globals
    Given a fresh ffkv server
    Given the following commands succeed:
      """
      [["MODULE","LOAD","{{module_fixture}}"]]
      """
    When I execute:
      """
      ["fixture.sum","7","-2","10"]
      """
    Then the reply equals:
      """
      15
      """
    When I execute:
      """
      ["fixture.counter"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["fixture.counter"]
      """
    Then the reply equals:
      """
      2
      """
    Given the following commands succeed:
      """
      [["MODULE","UNLOAD","fixture"]]
      """
    When I execute:
      """
      ["fixture.counter"]
      """
    Then the reply is an error with code "ERR"
    Given the following commands succeed:
      """
      [["MODULE","LOAD","{{module_fixture}}"]]
      """
    When I execute:
      """
      ["fixture.counter"]
      """
    Then the reply equals:
      """
      1
      """

  @command_MODULE_LOADEX
  Scenario: Native string configuration setters and load argv preserve binary data
    Given a fresh ffkv server
    Given the following commands succeed:
      """
      [["MODULE","LOADEX","{{module_fixture}}","CONFIG","prefix",{"base64":"AP8="},"ARGS","first",{"base64":"/wA="}]]
      """
    When I execute:
      """
      ["fixture.echo",{"base64":"aGkA"}]
      """
    Then the reply equals:
      """
      {"type":"bytes","base64":"AP9oaQA="}
      """
    When I execute:
      """
      ["fixture.initialization"]
      """
    Then the reply equals:
      """
      ["first",{"type":"bytes","base64":"/wA="}]
      """

  @command_MODULE_LOAD
  Scenario: Duplicate native loads do not reinitialize existing module globals
    Given a fresh ffkv server
    Given the following commands succeed:
      """
      [["MODULE","LOAD","{{module_fixture}}"],["fixture.counter"]]
      """
    When I execute:
      """
      ["MODULE","LOAD","{{module_fixture}}"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["fixture.counter"]
      """
    Then the reply equals:
      """
      2
      """

  @command_MODULE_LOAD
  Scenario Outline: Failed native initialization never publishes partial registrations
    Given a fresh ffkv server
    When I execute:
      """
      ["MODULE","LOAD","{{module_fixture}}","<mode>"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["fixture.echo","not registered"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["MODULE","LIST"]
      """
    Then the reply equals:
      """
      []
      """
    Given the following commands succeed:
      """
      [["MODULE","LOAD","{{module_fixture}}"]]
      """
    When I execute:
      """
      ["fixture.echo","registered after clean reload"]
      """
    Then the reply equals:
      """
      "registered after clean reload"
      """
    Examples:
      | mode           |
      | --partial-fail |
      | --unknown-api  |

  @command_MODULE_LOAD
  Scenario: Missing native libraries fail without fake built-in module registrations
    Given a fresh ffkv server
    When I execute:
      """
      ["MODULE","LOAD","{{directory}}/missing-module.so"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["MODULE","LIST"]
      """
    Then the reply equals:
      """
      []
      """

  @command_MODULE_LOADEX
  Scenario: Unknown native configuration names abort loading
    Given a fresh ffkv server
    When I execute:
      """
      ["MODULE","LOADEX","{{module_fixture}}","CONFIG","unknown","value"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["fixture.echo","not registered"]
      """
    Then the reply is an error with code "ERR"

  @command_MODULE_UNLOAD
  Scenario: A real OnUnload refusal preserves the library and command registration
    Given a fresh ffkv server
    Given the following commands succeed:
      """
      [["MODULE","LOAD","{{module_fixture}}","--unload-marker","{{directory}}/module-unloaded"],["fixture.unload","1"]]
      """
    When I execute:
      """
      ["MODULE","UNLOAD","fixture"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["fixture.echo","still live"]
      """
    Then the reply equals:
      """
      "still live"
      """
    Given the following commands succeed:
      """
      [["fixture.unload","0"],["MODULE","UNLOAD","fixture"]]
      """
    Then the file "{{directory}}/module-unloaded" exists
    When I execute:
      """
      ["fixture.echo","gone"]
      """
    Then the reply is an error with code "ERR"

  @command_MODULE_UNLOAD
  Scenario: An active native callback pins its library until it returns
    Given a fresh ffkv server
    Given the following commands succeed:
      """
      [["MODULE","LOAD","{{module_fixture}}"]]
      """
    When client "native" sends:
      """
      ["fixture.hold","{{directory}}/module-active","1500"]
      """
    Then the file "{{directory}}/module-active" exists
    When I execute:
      """
      ["MODULE","UNLOAD","fixture"]
      """
    Then the reply is an error with code "ERR"
    Then client "native" receives:
      """
      "OK"
      """
    When I execute:
      """
      ["MODULE","UNLOAD","fixture"]
      """
    Then the reply equals:
      """
      "OK"
      """

  @command_MODULE_LOAD @command_ACL_SETUSER
  Scenario: Native registered command flags participate in ACL authorization
    Given a fresh ffkv server
    Given the following commands succeed:
      """
      [["MODULE","LOAD","{{module_fixture}}"],["ACL","SETUSER","extensionlimited","on",">secret","+@all","-@admin","~*"]]
      """
    When client "limited" executes:
      """
      ["AUTH","extensionlimited","secret"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "limited" executes:
      """
      ["fixture.echo","must be denied"]
      """
    Then the reply is an error with code "NOPERM"
    Given the following commands succeed:
      """
      [["ACL","SETUSER","extensionlimited","+fixture.echo"]]
      """
    When client "limited" executes:
      """
      ["fixture.echo","permitted"]
      """
    Then the reply equals:
      """
      "permitted"
      """

  @command_MODULE_LOAD
  Scenario: Native command arity and error replies do not unload the library
    Given a fresh ffkv server
    Given the following commands succeed:
      """
      [["MODULE","LOAD","{{module_fixture}}"]]
      """
    When I execute:
      """
      ["fixture.echo"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["fixture.fail"]
      """
    Then the reply is an error with code "EXTENSION"
    When I execute:
      """
      ["fixture.sum","4","5"]
      """
    Then the reply equals:
      """
      9
      """

  @command_MODULE_LOAD
  Scenario: Native Call and key handles mutate the actual durable engine
    Given a fresh ffkv server
    Given the following commands succeed:
      """
      [["MODULE","LOAD","{{module_fixture}}"],["fixture.keyset","module:value","41"]]
      """
    When I execute:
      """
      ["fixture.incr","module:value"]
      """
    Then the reply equals:
      """
      42
      """
    When I execute:
      """
      ["GET","module:value"]
      """
    Then the reply equals:
      """
      "42"
      """
    When ffkv restarts after a crash
    When I execute:
      """
      ["GET","module:value"]
      """
    Then the reply equals:
      """
      "42"
      """

  @command_MODULE_LOAD @command_ACL_DRYRUN
  Scenario: Native key metadata enforces ACL key patterns and nested Call permissions
    Given a fresh ffkv server
    Given the following commands succeed:
      """
      [["MODULE","LOAD","{{module_fixture}}"],["SET","allowed:item","visible"],["SET","private:item","secret"],["ACL","SETUSER","module-reader","on",">secret","+fixture.get","~allowed:*"]]
      """
    When I execute:
      """
      ["ACL","DRYRUN","module-reader","fixture.get","private:item"]
      """
    Then the reply describes an ACL dry-run denial
    When client "module-reader" executes:
      """
      ["AUTH","module-reader","secret"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "module-reader" executes:
      """
      ["fixture.get","allowed:item"]
      """
    Then the reply is an error with code "NOPERM"
    Given the following commands succeed:
      """
      [["ACL","SETUSER","module-reader","+get"]]
      """
    When client "module-reader" executes:
      """
      ["fixture.get","allowed:item"]
      """
    Then the reply equals:
      """
      "visible"
      """
    When client "module-reader" executes:
      """
      ["fixture.get","private:item"]
      """
    Then the reply is an error with code "NOPERM"

  @command_MODULE_LOAD
  Scenario: Native commands run inside transactions and scripts unless their registered flags forbid it
    Given a fresh ffkv server
    Given the following commands succeed:
      """
      [["MODULE","LOAD","{{module_fixture}}"],["MULTI"],["fixture.keyset","module:transaction","8"],["fixture.incr","module:transaction"]]
      """
    When I execute:
      """
      ["EXEC"]
      """
    Then the reply equals:
      """
      ["OK",9]
      """
    When I execute:
      """
      ["EVAL","return redis.call('fixture.get',KEYS[1])","1","module:transaction"]
      """
    Then the reply equals:
      """
      "9"
      """
    When I execute:
      """
      ["EVAL","return redis.call('fixture.hold',ARGV[1],'1')","0","{{directory}}/not-created"]
      """
    Then the reply is an error with code "ERR"

  @command_MODULE_LOAD
  Scenario: Native command metadata exposes actual arity flags and key positions
    Given a fresh ffkv server
    Given the following commands succeed:
      """
      [["MODULE","LOAD","{{module_fixture}}"]]
      """
    When I execute:
      """
      ["COMMAND","GETKEYS","fixture.keyset","module:metadata","value"]
      """
    Then the reply equals:
      """
      ["module:metadata"]
      """
    When I execute:
      """
      ["EVAL","local c=redis.call('COMMAND','INFO','fixture.keyset')[1];local f={};for _,v in ipairs(c[3]) do f[v]=1 end;return {c[1],c[2],c[4],c[5],c[6],f.write or 0,f.denyoom or 0,f.module or 0}","0"]
      """
    Then the reply equals:
      """
      ["fixture.keyset",3,1,1,1,1,1,1]
      """

  @command_ACL_SETUSER @command_ACL_DRYRUN
  Scenario: ACL dry runs exercise disabled user permissions without mutating data
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["ACL","SETUSER","dry-reader","off","+set","~scratch:*"]]
      """
    When I execute:
      """
      ["ACL","DRYRUN","dry-reader","SET","scratch:one","not-written"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["GET","scratch:one"]
      """
    Then the reply is null

  @command_ACL_SETUSER @command_ACL_WHOAMI
  Scenario: Existing authenticated sessions observe incremental ACL changes
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["SET","tenant:item","original"],["ACL","SETUSER","reader","on",">secret","+get","+acl|whoami","~tenant:*"]]
      """
    When client "reader" executes:
      """
      ["AUTH","reader","secret"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "reader" executes:
      """
      ["ACL","WHOAMI"]
      """
    Then the reply equals:
      """
      "reader"
      """
    When client "reader" executes:
      """
      ["GET","tenant:item"]
      """
    Then the reply equals:
      """
      "original"
      """
    When I execute:
      """
      ["ACL","SETUSER","reader","-get"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "reader" executes:
      """
      ["GET","tenant:item"]
      """
    Then the reply is an error with code "NOPERM"

  @command_ACL_SETUSER
  Scenario: Off blocks new authentication without revoking existing sessions
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["ACL","SETUSER","existing","on",">secret","+ping"]]
      """
    When client "existing" executes:
      """
      ["AUTH","existing","secret"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["ACL","SETUSER","existing","off"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "existing" executes:
      """
      ["PING"]
      """
    Then the reply equals:
      """
      "PONG"
      """
    When client "new-login" executes:
      """
      ["AUTH","existing","secret"]
      """
    Then the reply is an error with code "WRONGPASS"

  @command_ACL_SETUSER
  Scenario: Password removal and resetpass revoke credentials rather than allowing empty passwords
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["ACL","SETUSER","rotating","on",">old",">new","+ping"],["ACL","SETUSER","rotating","<old"]]
      """
    When client "old-password" executes:
      """
      ["AUTH","rotating","old"]
      """
    Then the reply is an error with code "WRONGPASS"
    When client "new-password" executes:
      """
      ["AUTH","rotating","new"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["ACL","SETUSER","rotating","nopass","resetpass"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "empty-password" executes:
      """
      ["AUTH","rotating",""]
      """
    Then the reply is an error with code "WRONGPASS"

  @command_ACL_SETUSER @command_ACL_GETUSER
  Scenario: Hashed passwords are genuine credentials and can be removed by hash
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["ACL","SETUSER","hashed","on","#2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824","+ping"]]
      """
    When client "hash-login" executes:
      """
      ["AUTH","hashed","hello"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["ACL","SETUSER","hashed","!2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "removed-hash" executes:
      """
      ["AUTH","hashed","hello"]
      """
    Then the reply is an error with code "WRONGPASS"

  @command_ACL_SETUSER @command_ACL_GETUSER
  Scenario: Selectors expose their real rules and cannot combine permissions across selectors
    Given a fresh ffkv server
    And client "admin" uses RESP 3
    And the following commands succeed:
      """
      [["ACL","SETUSER","selected","on",">secret","+get","~read:*","(+set ~write:*)"]]
      """
    When client "admin" executes:
      """
      ["ACL","GETUSER","selected"]
      """
    Then the reply contains:
      """
      {"commands":"-@all +get","keys":"~read:*","channels":"","selectors":[{"commands":"-@all +set","keys":"~write:*","channels":""}]}
      """
    When client "selected" executes:
      """
      ["AUTH","selected","secret"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "selected" executes:
      """
      ["SET","write:one","allowed"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "selected" executes:
      """
      ["SET","read:one","denied"]
      """
    Then the reply is an error with code "NOPERM"
    When client "selected" executes:
      """
      ["GET","write:one"]
      """
    Then the reply is an error with code "NOPERM"

  @command_ACL_SETUSER
  Scenario: Read and write key patterns independently constrain the same command grant
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["SET","input:one","source"],["ACL","SETUSER","directional","on",">secret","+get","+set","%R~input:*","%W~output:*"]]
      """
    When client "directional" executes:
      """
      ["AUTH","directional","secret"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "directional" executes:
      """
      ["GET","input:one"]
      """
    Then the reply equals:
      """
      "source"
      """
    When client "directional" executes:
      """
      ["SET","output:one","target"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "directional" executes:
      """
      ["SET","input:one","denied"]
      """
    Then the reply is an error with code "NOPERM"
    When client "directional" executes:
      """
      ["GET","output:one"]
      """
    Then the reply is an error with code "NOPERM"

  @command_ACL_SETUSER
  Scenario: PubSub command grants cannot borrow channel grants from another selector
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["ACL","SETUSER","publisher","on",">secret","+publish","&allowed:*","(+get &forbidden:*)"]]
      """
    When client "publisher" executes:
      """
      ["AUTH","publisher","secret"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "publisher" executes:
      """
      ["PUBLISH","allowed:news","message"]
      """
    Then the reply equals:
      """
      0
      """
    When client "publisher" executes:
      """
      ["PUBLISH","forbidden:news","message"]
      """
    Then the reply is an error with code "NOPERM"

  @command_ACL_CAT
  Scenario: ACL category membership is derived from registered command classifications
    Given a fresh ffkv server
    When I execute:
      """
      ["ACL","CAT","string"]
      """
    Then the reply contains:
      """
      ["get","set"]
      """
    When I execute:
      """
      ["ACL","CAT","not-a-category"]
      """
    Then the reply is an error with code "ERR"

  @command_ACL_LIST @command_ACL_USERS @command_ACL_DELUSER @command_ACL_GETUSER
  Scenario: User enumeration and removal reflect live state without deleting default
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["ACL","SETUSER","enumerated","off"]]
      """
    When I execute:
      """
      ["ACL","USERS"]
      """
    Then the reply contains:
      """
      ["default","enumerated"]
      """
    When I execute:
      """
      ["ACL","LIST"]
      """
    Then the reply contains:
      """
      ["user enumerated off resetchannels -@all"]
      """
    When I execute:
      """
      ["ACL","DELUSER","enumerated","missing"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["ACL","GETUSER","enumerated"]
      """
    Then the reply is null
    When I execute:
      """
      ["ACL","DELUSER","default"]
      """
    Then the reply is an error with code "ERR"

  @command_ACL_LOG
  Scenario: ACL log records failed authentication without disclosing supplied passwords
    Given a fresh ffkv server
    And client "security-admin" uses RESP 3
    And the following commands succeed:
      """
      [["ACL","LOG","RESET"],["ACL","SETUSER","audit-user","on",">correct","+ping"]]
      """
    When client "bad-login" executes:
      """
      ["AUTH","audit-user","never-log-this-password"]
      """
    Then the reply is an error with code "WRONGPASS"
    When client "security-admin" executes:
      """
      ["ACL","LOG","1"]
      """
    Then the reply contains:
      """
      [{"count":1,"reason":"auth","context":"toplevel","object":"AUTH","username":"audit-user"}]
      """
    When I execute:
      """
      ["ACL","LOG","RESET"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["ACL","LOG"]
      """
    Then the reply equals:
      """
      []
      """

  @command_ACL_GENPASS
  Scenario Outline: Generated passwords expose the requested rounded cryptographic entropy
    Given a fresh ffkv server
    When I execute:
      """
      ["ACL","GENPASS","<bits>"]
      """
    Then the reply is a hexadecimal string of length <length>
    Examples:
      | bits | length |
      | 1    | 1      |
      | 5    | 2      |
      | 256  | 64     |
      | 1024 | 256    |

  @command_ACL_GENPASS
  Scenario: Password entropy requests outside the supported range are rejected
    Given a fresh ffkv server
    When I execute:
      """
      ["ACL","GENPASS","1025"]
      """
    Then the reply is an error with code "ERR"

  @command_ACL_SAVE @command_ACL_LOAD
  Scenario: ACL file operations reject an unconfigured path instead of pretending to persist
    Given a fresh ffkv server
    When I execute:
      """
      ["ACL","SAVE"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["ACL","LOAD"]
      """
    Then the reply is an error with code "ERR"

  @command_ACL_SAVE @command_ACL_LOAD
  Scenario: Saved ACL rules reload atomically and survive a server restart
    Given a fresh ffkv server with ACL rules:
      """
      user default on nopass ~* &* +@all
      """
    And the following commands succeed:
      """
      [["SET","persisted:acl","value"],["ACL","SETUSER","persistent-reader","on",">saved-secret","+get","~persisted:*"],["ACL","SAVE"],["ACL","SETUSER","persistent-reader","-get"],["ACL","SETUSER","unsaved-user","on","nopass","+ping"]]
      """
    When client "saved-reader" executes:
      """
      ["AUTH","persistent-reader","saved-secret"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "saved-reader" executes:
      """
      ["GET","persisted:acl"]
      """
    Then the reply is an error with code "NOPERM"
    When I execute:
      """
      ["ACL","LOAD"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "saved-reader" executes:
      """
      ["GET","persisted:acl"]
      """
    Then the reply equals:
      """
      "value"
      """
    When I execute:
      """
      ["ACL","GETUSER","unsaved-user"]
      """
    Then the reply is null
    When ffkv restarts after a crash
    When client "restarted-reader" executes:
      """
      ["AUTH","persistent-reader","saved-secret"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "restarted-reader" executes:
      """
      ["GET","persisted:acl"]
      """
    Then the reply equals:
      """
      "value"
      """
