Feature: Bounded Lua scripts and durable named function libraries

  @command_EVAL
  Scenario: A script sees its own writes and earlier effects survive a later Lua error
    Given a fresh ffkv server
    When I execute:
      """
      ["EVAL","redis.call('SET',KEYS[1],ARGV[1]); error('after write')","1","script:key","durable"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["EVAL","return {redis.call('GET',KEYS[1]),ARGV[1]}","1","script:key","argument"]
      """
    Then the reply equals:
      """
      ["durable","argument"]
      """
    When ffkv restarts after a crash
    When I execute:
      """
      ["GET","script:key"]
      """
    Then the reply equals:
      """
      "durable"
      """

  @command_SCRIPT_LOAD @command_SCRIPT_EXISTS @command_EVALSHA @command_SCRIPT_FLUSH
  Scenario: Compiled script cache lookup is separate from durable data
    Given a fresh ffkv server
    When I execute:
      """
      ["SCRIPT","LOAD","return redis.call('INCR',KEYS[1])"]
      """
    When I save the reply as "digest"
    When I execute:
      """
      ["SCRIPT","EXISTS",{"saved":"digest"},"0000000000000000000000000000000000000000"]
      """
    Then the reply equals:
      """
      [1,0]
      """
    When I execute:
      """
      ["EVALSHA",{"saved":"digest"},"1","counter"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["SCRIPT","FLUSH","ASYNC"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["EVALSHA",{"saved":"digest"},"1","counter"]
      """
    Then the reply is an error with code "NOSCRIPT"
    When I execute:
      """
      ["GET","counter"]
      """
    Then the reply equals:
      """
      "1"
      """

  @command_EVAL_RO @command_EVALSHA_RO
  Scenario: Readonly scripts reject writes even through protected calls
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["SET","readonly:key","original"]]
      """
    When I execute:
      """
      ["EVAL_RO","return redis.call('GET',KEYS[1])","1","readonly:key"]
      """
    Then the reply equals:
      """
      "original"
      """
    When I execute:
      """
      ["SCRIPT","LOAD","return redis.call('GET',KEYS[1])"]
      """
    When I save the reply as "cached_read_digest"
    When I execute:
      """
      ["EVALSHA_RO",{"saved":"cached_read_digest"},"1","readonly:key"]
      """
    Then the reply equals:
      """
      "original"
      """
    When I execute:
      """
      ["SCRIPT","LOAD","return redis.pcall('SET',KEYS[1],'forbidden')"]
      """
    When I save the reply as "readonly_digest"
    When I execute:
      """
      ["EVALSHA_RO",{"saved":"readonly_digest"},"1","readonly:key"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["GET","readonly:key"]
      """
    Then the reply equals:
      """
      "original"
      """

  @command_FUNCTION_LOAD @command_FCALL @command_FCALL_RO @command_FUNCTION_LIST
  Scenario: Named callbacks share local helpers and expose registration metadata
    Given a fresh ffkv server
    Given client "metadata" uses RESP 3
    When I execute:
      """
      ["FUNCTION","LOAD","#!lua name=inventory\nlocal call = redis.call\nlocal function get(keys) return call('GET', keys[1]) end\nredis.register_function{function_name='inventory_get',callback=get,flags={'no-writes'},description='Read an item'}"]
      """
    Then the reply equals:
      """
      "inventory"
      """
    And the following commands succeed:
      """
      [["FUNCTION","LOAD","#!lua name=writer\nredis.register_function('inventory_put',function(keys,args) return redis.call('SET',keys[1],args[1]) end)"]]
      """
    When I execute:
      """
      ["FCALL","inventory_put","1","item:42","present"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["FCALL_RO","inventory_get","1","item:42"]
      """
    Then the reply equals:
      """
      "present"
      """
    When client "metadata" executes:
      """
      ["FUNCTION","LIST","LIBRARYNAME","invent[oa]ry"]
      """
    Then the reply equals:
      """
      [{"library_name":"inventory","engine":"LUA","functions":[{"name":"inventory_get","description":"Read an item","flags":["no-writes"]}]}]
      """
    When I execute:
      """
      ["FCALL_RO","inventory_put","1","item:42","wrong"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["GET","item:42"]
      """
    Then the reply equals:
      """
      "present"
      """

  @command_FUNCTION_LOAD @command_FUNCTION_LIST
  Scenario: Invalid replacement cannot discard the previous library
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["FUNCTION","LOAD","#!lua name=library\nredis.register_function('answer',function() return 42 end)"]]
      """
    When I execute:
      """
      ["FUNCTION","LOAD","REPLACE","#!lua name=library\nredis.register_function('answer',function() return 99 end); error('invalid replacement')"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["FCALL","answer","0"]
      """
    Then the reply equals:
      """
      42
      """
    When I execute:
      """
      ["FUNCTION","LOAD","#!lua name=empty\n"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["FUNCTION","LIST","LIBRARYNAME","empty","WITHCODE"]
      """
    Then the reply equals:
      """
      []
      """

  @command_FUNCTION_DUMP @command_FUNCTION_RESTORE @command_FUNCTION_DELETE @command_FUNCTION_FLUSH
  Scenario: Function containers restore with append replace and flush policies
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["FUNCTION","LOAD","#!lua name=versioned\nredis.register_function('version',function() return 1 end)"]]
      """
    When I execute:
      """
      ["FUNCTION","DUMP"]
      """
    When I save the reply as "libraries"
    When I execute:
      """
      ["FUNCTION","RESTORE",{"saved":"libraries"},"APPEND"]
      """
    Then the reply is an error with code "ERR"
    And the following commands succeed:
      """
      [["FUNCTION","LOAD","REPLACE","#!lua name=versioned\nredis.register_function('version',function() return 2 end)"],["FUNCTION","LOAD","#!lua name=extra\nredis.register_function('other',function() return 'extra' end)"]]
      """
    When I execute:
      """
      ["FUNCTION","RESTORE",{"saved":"libraries"},"REPLACE"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["FCALL","version","0"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["FCALL","other","0"]
      """
    Then the reply equals:
      """
      "extra"
      """
    When I execute:
      """
      ["FUNCTION","RESTORE",{"saved":"libraries"},"FLUSH"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["FCALL","other","0"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["FUNCTION","DELETE","versioned"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["FUNCTION","RESTORE",{"saved":"libraries"}]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["FUNCTION","FLUSH","SYNC"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["FUNCTION","LIST"]
      """
    Then the reply equals:
      """
      []
      """

  @command_FUNCTION_RESTORE @command_FUNCTION_LOAD @command_FCALL
  Scenario: Libraries survive crash recovery but invalid containers cannot alter them
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["FUNCTION","LOAD","#!lua name=persistent\nredis.register_function('increment',function(keys) return redis.call('INCR',keys[1]) end)"],["FCALL","increment","1","persistent:count"]]
      """
    When I execute:
      """
      ["FUNCTION","RESTORE",{"base64":"9QABAgMEBQYHCAkK"},"FLUSH"]
      """
    Then the reply is an error with code "ERR"
    When ffkv restarts after a crash
    When I execute:
      """
      ["FCALL","increment","1","persistent:count"]
      """
    Then the reply equals:
      """
      2
      """
    When I execute:
      """
      ["FUNCTION","DELETE","persistent"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When ffkv restarts after a crash
    When I execute:
      """
      ["FCALL","increment","0"]
      """
    Then the reply is an error with code "ERR"

  @command_FUNCTION_STATS @command_FUNCTION_KILL @command_SCRIPT_KILL
  Scenario: Function engine statistics reflect load delete and idle cancellation
    Given a fresh ffkv server
    Given client "statistics" uses RESP 3
    And the following commands succeed:
      """
      [["FUNCTION","LOAD","#!lua name=stats\nredis.register_function('one',function() return 1 end); redis.register_function('two',function() return 2 end)"]]
      """
    When client "statistics" executes:
      """
      ["FUNCTION","STATS"]
      """
    Then the reply equals:
      """
      {"running_script":null,"engines":{"LUA":{"libraries_count":1,"functions_count":2}}}
      """
    When I execute:
      """
      ["FUNCTION","KILL"]
      """
    Then the reply is an error with code "NOTBUSY"
    When I execute:
      """
      ["SCRIPT","KILL"]
      """
    Then the reply is an error with code "NOTBUSY"
    And the following commands succeed:
      """
      [["FUNCTION","DELETE","stats"]]
      """
    When client "statistics" executes:
      """
      ["FUNCTION","STATS"]
      """
    Then the reply equals:
      """
      {"running_script":null,"engines":{"LUA":{"libraries_count":0,"functions_count":0}}}
      """

  @command_SCRIPT_DEBUG @command_SCRIPT_KILL
  Scenario: A line debugger pauses a live script and kill interrupts its eligible execution
    Given a fresh ffkv server
    When client "debugger" executes:
      """
      ["SCRIPT","DEBUG","SYNC"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "debugger" sends:
      """
      ["EVAL","local n = 1\nreturn n + 1","0"]
      """
    Then client "debugger" receives:
      """
      ["* Stopped at 1","-> 1 local n = 1"]
      """
    When I execute:
      """
      ["GET","debugged"]
      """
    Then the reply is an error with code "BUSY"
    When I execute:
      """
      ["SCRIPT","KILL"]
      """
    Then the reply equals:
      """
      "OK"
      """
    Then client "debugger" receives:
      """
      ["<endsession>"]
      """
    Then client "debugger" receives an error with code "ERR"
    When I execute:
      """
      ["EVAL","return 7","0"]
      """
    Then the reply equals:
      """
      7
      """

  @command_SCRIPT_DEBUG @command_SCRIPT_KILL
  Scenario: Debug stepping exposes the first write and prevents unsafe cancellation
    Given a fresh ffkv server
    When client "debugger" executes:
      """
      ["SCRIPT","DEBUG","SYNC"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "debugger" sends:
      """
      ["EVAL","redis.call('SET','debug:key','kept')\nreturn 'finished'","0"]
      """
    Then client "debugger" receives:
      """
      ["* Stopped at 1","-> 1 redis.call('SET','debug:key','kept')"]
      """
    When client "debugger" sends:
      """
      ["step"]
      """
    Then client "debugger" receives:
      """
      ["* Stopped at 2","-> 2 return 'finished'"]
      """
    When I execute:
      """
      ["SCRIPT","KILL"]
      """
    Then the reply is an error with code "UNKILLABLE"
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
      "finished"
      """
    When I execute:
      """
      ["GET","debug:key"]
      """
    Then the reply equals:
      """
      "kept"
      """

  @command_SCRIPT_DEBUG
  Scenario: Fork style debug execution discards effects while retaining a useful result
    Given a fresh ffkv server
    When client "debugger" executes:
      """
      ["SCRIPT","DEBUG","YES"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "debugger" sends:
      """
      ["EVAL","redis.call('SET','debug:discard','temporary')\nreturn redis.call('GET','debug:discard')","0"]
      """
    Then client "debugger" receives:
      """
      ["* Stopped at 1","-> 1 redis.call('SET','debug:discard','temporary')"]
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
      "temporary"
      """
    When I execute:
      """
      ["GET","debug:discard"]
      """
    Then the reply is null

  @command_FUNCTION_KILL @command_FUNCTION_STATS @command_FCALL_RO
  Scenario: An executing readonly function is observable and can actually be interrupted
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["FUNCTION","LOAD","#!lua name=interruptible\nredis.register_function{function_name='busy',flags={'no-writes'},callback=function() local s=string.rep('x',1048576); while true do s=string.gsub(s,'x','x') end end}\nredis.register_function('healthy',function() return 'alive' end)"]]
      """
    When client "runner" sends:
      """
      ["FCALL_RO","busy","0"]
      """
    Then function "busy" is running
    When I execute:
      """
      ["FUNCTION","KILL"]
      """
    Then the reply equals:
      """
      "OK"
      """
    Then client "runner" receives an error with code "ERR"
    When I execute:
      """
      ["FCALL","healthy","0"]
      """
    Then the reply equals:
      """
      "alive"
      """

  @command_FUNCTION_KILL @command_FCALL
  Scenario: A function that has written cannot be killed and retains its completed effects
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["FUNCTION","LOAD","#!lua name=unkillable\nredis.register_function('busy_write',function(keys) redis.call('SET',keys[1],'kept'); local s=string.rep('x',1048576); for i=1,100 do s=string.gsub(s,'x','x') end; return 'finished' end)"]]
      """
    When client "runner" sends:
      """
      ["FCALL","busy_write","1","function:written"]
      """
    Then function "busy_write" is running
    When I execute:
      """
      ["FUNCTION","KILL"]
      """
    Then the reply is an error with code "UNKILLABLE"
    Then client "runner" receives:
      """
      "finished"
      """
    When I execute:
      """
      ["GET","function:written"]
      """
    Then the reply equals:
      """
      "kept"
      """

  @command_FUNCTION_LOAD
  Scenario: Replace policy cannot steal functions owned by a different library
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["FUNCTION","LOAD","#!lua name=first\nredis.register_function('first_answer',function() return 1 end)"],["FUNCTION","LOAD","#!lua name=second\nredis.register_function('second_answer',function() return 2 end)"]]
      """
    When I execute:
      """
      ["FUNCTION","LOAD","REPLACE","#!lua name=first\nredis.register_function('second_answer',function() return 99 end)"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["FCALL","first_answer","0"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["FCALL","second_answer","0"]
      """
    Then the reply equals:
      """
      2
      """

  @command_FCALL_RO @command_EVAL
  Scenario: A no-writes declaration is enforced rather than trusted
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["FUNCTION","LOAD","#!lua name=dishonest\nredis.register_function{function_name='readonly_write',flags={'no-writes'},callback=function(keys) return redis.pcall('SET',keys[1],'forbidden') end}"]]
      """
    When I execute:
      """
      ["FCALL_RO","readonly_write","1","readonly:missing"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["EVAL","#!lua flags=no-writes\nreturn redis.call('SET',KEYS[1],'forbidden')","1","readonly:missing"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["EXISTS","readonly:missing"]
      """
    Then the reply equals:
      """
      0
      """

  @command_EVAL @command_FCALL
  Scenario: Each outer Lua invocation owns a fresh import session rather than borrowing network layouts
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["HIMPORT","PREPARE","layout","network_field"],["FUNCTION","LOAD","#!lua name=imports\nredis.register_function('import_row',function(keys,args) if args[1]=='prepare' then redis.call('HIMPORT','PREPARE','layout','lua_field') end; return redis.call('HIMPORT','SET',keys[1],'layout',args[2]) end)"]]
      """
    When I execute:
      """
      ["EVAL","return redis.call('HIMPORT','SET',KEYS[1],'layout','must-not-import')","1","lua:no-layout"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["EVAL","redis.call('HIMPORT','PREPARE','layout','lua_field'); redis.call('HIMPORT','SET',KEYS[1],'layout','script'); return redis.call('HGET',KEYS[1],'lua_field')","1","lua:script"]
      """
    Then the reply equals:
      """
      "script"
      """
    When I execute:
      """
      ["FCALL","import_row","1","lua:function","without-prepare","must-not-import"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["FCALL","import_row","1","lua:function","prepare","function"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["FCALL","import_row","1","lua:next-function","without-prepare","must-not-import"]
      """
    Then the reply is an error with code "ERR"
    And the following commands succeed:
      """
      [["HIMPORT","SET","network:row","layout","network"]]
      """
    When I execute:
      """
      ["HMGET","network:row","network_field","lua_field"]
      """
    Then the reply equals:
      """
      ["network",null]
      """
    When I execute:
      """
      ["EVAL","redis.call('HIMPORT','PREPARE','error_layout','unused'); error('abort')","0"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["EVAL","return redis.call('HIMPORT','SET',KEYS[1],'error_layout','must-not-import')","1","lua:after-error"]
      """
    Then the reply is an error with code "ERR"

  @command_FUNCTION_LIST @command_FUNCTION_LOAD
  Scenario: Library source and optional metadata roundtrip through WITHCODE
    Given a fresh ffkv server
    Given client "metadata" uses RESP 3
    And the following commands succeed:
      """
      [["FUNCTION","LOAD","#!lua name=source\nredis.register_function('source_answer',function() return 42 end)"]]
      """
    When client "metadata" executes:
      """
      ["FUNCTION","LIST","WITHCODE","LIBRARYNAME","sour*"]
      """
    Then the reply equals:
      """
      [{"library_name":"source","engine":"LUA","functions":[{"name":"source_answer","description":null,"flags":[]}],"library_code":"#!lua name=source\nredis.register_function('source_answer',function() return 42 end)"}]
      """
    When I execute:
      """
      ["FUNCTION","LOAD","#!lua name=invalid_flags\nredis.register_function{function_name='invalid',callback=function() return 0 end,flags={'unknown'}}"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["FUNCTION","LOAD","#!lua name=invalid_global\naccidental_global=1; redis.register_function('invalid',function() return 0 end)"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["FCALL","source_answer","0"]
      """
    Then the reply equals:
      """
      42
      """

  @command_SCRIPT_DEBUG
  Scenario: Debug mode validates its argument and NO restores ordinary execution
    Given a fresh ffkv server
    When I execute:
      """
      ["SCRIPT","DEBUG","invalid"]
      """
    Then the reply is an error with code "ERR"
    And the following commands succeed:
      """
      [["SCRIPT","DEBUG","SYNC"],["SCRIPT","DEBUG","NO"]]
      """
    When I execute:
      """
      ["EVAL","redis.debug('ordinary execution'); return 42","0"]
      """
    Then the reply equals:
      """
      42
      """

  @command_EVAL_RO @command_EVALSHA_RO @command_SCRIPT_LOAD
  Scenario Outline: Readonly script admission preserves legacy and explicit shebang metadata
    Given a fresh ffkv server
    When I execute:
      """
      ["EVAL_RO","<source>",0]
      """
    Then the reply equals:
      """
      <reply>
      """
    When I execute:
      """
      ["SCRIPT","LOAD","<source>"]
      """
    When I save the reply as "readonly-metadata-digest"
    When I execute:
      """
      ["EVALSHA_RO",{"saved":"readonly-metadata-digest"},0]
      """
    Then the reply equals:
      """
      <reply>
      """

    Examples:
      | source                                | reply                                                                                                  |
      | return 42                             | 42                                                                                                     |
      | #!lua flags=no-writes\nreturn 42        | 42                                                                                                     |

  @command_EVAL @command_EVALSHA @command_EVAL_RO @command_EVALSHA_RO @command_SCRIPT_LOAD
  Scenario: A valid write-capable shebang script rejects readonly entry points
    Given a fresh ffkv server
    When I execute:
      """
      ["EVAL","#!lua\nreturn 42",0]
      """
    Then the reply equals:
      """
      42
      """
    When I execute:
      """
      ["EVAL_RO","#!lua\nreturn 42",0]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["SCRIPT","LOAD","#!lua\nreturn 42"]
      """
    When I save the reply as "write-capable-digest"
    When I execute:
      """
      ["EVALSHA",{"saved":"write-capable-digest"},0]
      """
    Then the reply equals:
      """
      42
      """
    When I execute:
      """
      ["EVALSHA_RO",{"saved":"write-capable-digest"},0]
      """
    Then the reply is an error with code "ERR"
