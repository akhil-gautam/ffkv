# Contracts: https://redis.io/docs/latest/commands/json.set/; https://redis.io/docs/latest/commands/json.resp/; https://redis.io/docs/latest/commands/json.debug-memory/
Feature: JSONPath operations transform document values atomically

  @command_JSON_SET @command_JSON_GET @command_JSON_TYPE
  Scenario: Conditional path assignment selects values rather than returning stored source text
    Given a fresh ffkv server
    When I execute:
      """
      ["JSON.SET","doc","$","{\"items\":[{\"price\":2},{\"price\":9}],\"name\":\"book\"}"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["JSON.SET","doc","$.name","\"ignored\"","NX"]
      """
    Then the reply is null
    When I execute:
      """
      ["JSON.GET","doc","$.items[?(@.price > 5)].price"]
      """
    Then the reply equals:
      """
      "[9]"
      """
    When I execute:
      """
      ["JSON.TYPE","doc","$.items[*].price"]
      """
    Then the reply equals:
      """
      ["integer","integer"]
      """
    When I execute:
      """
      ["JSON.GET","doc","$.name"]
      """
    Then the reply equals:
      """
      "[\"book\"]"
      """
    When ffkv restarts after a crash
    When I execute:
      """
      ["JSON.GET","doc","$.items[*].price"]
      """
    Then the reply equals:
      """
      "[2,9]"
      """
  @command_JSON_ARRAPPEND @command_JSON_ARRINSERT @command_JSON_ARRINDEX @command_JSON_ARRLEN @command_JSON_ARRPOP @command_JSON_ARRTRIM
  Scenario: Array insertion indexing popping and trimming preserve JSON values
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["JSON.SET","doc","$","{\"a\":[1,3]}"]]
      """
    When I execute:
      """
      ["JSON.ARRINSERT","doc","$.a",1,"2"]
      """
    Then the reply equals:
      """
      [3]
      """
    When I execute:
      """
      ["JSON.ARRAPPEND","doc","$.a","4","{\"v\":5}"]
      """
    Then the reply equals:
      """
      [5]
      """
    When I execute:
      """
      ["JSON.ARRINDEX","doc","$.a","3"]
      """
    Then the reply equals:
      """
      [2]
      """
    When I execute:
      """
      ["JSON.ARRLEN","doc","$.a"]
      """
    Then the reply equals:
      """
      [5]
      """
    When I execute:
      """
      ["JSON.ARRPOP","doc","$.a",-1]
      """
    Then the reply equals:
      """
      ["{\"v\":5}"]
      """
    When I execute:
      """
      ["JSON.ARRTRIM","doc","$.a",1,2]
      """
    Then the reply equals:
      """
      [2]
      """
    When I execute:
      """
      ["JSON.GET","doc","$.a"]
      """
    Then the reply equals:
      """
      "[[2,3]]"
      """
    When I execute:
      """
      ["JSON.ARRINSERT","doc","$.a",99,"8"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["JSON.GET","doc","$.a"]
      """
    Then the reply equals:
      """
      "[[2,3]]"
      """
  @command_JSON_NUMINCRBY @command_JSON_NUMMULTBY @command_JSON_TOGGLE @command_JSON_STRAPPEND @command_JSON_STRLEN
  Scenario: Numeric boolean and string updates operate across matched nodes
    Given a fresh ffkv server
    And client "json3" uses RESP 3
    And the following commands succeed:
      """
      [["JSON.SET","doc","$","{\"n\":[2,4],\"enabled\":true,\"name\":\"red\"}"]]
      """
    When I execute:
      """
      ["JSON.NUMINCRBY","doc","$.n[*]",3]
      """
    Then the reply equals:
      """
      "[5,7]"
      """
    When I execute:
      """
      ["JSON.NUMMULTBY","doc","$.n[*]",2]
      """
    Then the reply equals:
      """
      "[10,14]"
      """
    When client "json3" executes:
      """
      ["JSON.TOGGLE","doc","$.enabled"]
      """
    Then the reply equals:
      """
      [0]
      """
    When I execute:
      """
      ["JSON.STRAPPEND","doc","$.name","\" fox\""]
      """
    Then the reply equals:
      """
      [7]
      """
    When I execute:
      """
      ["JSON.STRLEN","doc","$.name"]
      """
    Then the reply equals:
      """
      [7]
      """
    When I execute:
      """
      ["JSON.GET","doc","$.name"]
      """
    Then the reply equals:
      """
      "[\"red fox\"]"
      """
    When I execute:
      """
      ["JSON.GET","doc","$.enabled"]
      """
    Then the reply equals:
      """
      "[false]"
      """
  @command_JSON_OBJKEYS @command_JSON_OBJLEN @command_JSON_RESP
  Scenario: Object metadata and RESP expose the selected structure
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["JSON.SET","doc","$","{\"obj\":{\"only\":1},\"a\":[1,\"two\",null]}"]]
      """
    When I execute:
      """
      ["JSON.OBJKEYS","doc","$.obj"]
      """
    Then the reply equals:
      """
      [["only"]]
      """
    When I execute:
      """
      ["JSON.OBJLEN","doc","$.obj"]
      """
    Then the reply equals:
      """
      [1]
      """
    When I execute:
      """
      ["JSON.RESP","doc","$.a"]
      """
    Then the reply equals:
      """
      [["[",1,"two",null]]
      """
  @command_JSON_MERGE @command_JSON_DEL @command_JSON_FORGET @command_JSON_CLEAR
  Scenario: Merge patch deletion forgetting and clear apply distinct transformations
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["JSON.SET","doc","$","{\"obj\":{\"a\":1,\"b\":2},\"a\":[1,2],\"n\":7}"]]
      """
    When I execute:
      """
      ["JSON.MERGE","doc","$.obj","{\"a\":null,\"c\":3}"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["JSON.GET","doc","$.obj.a"]
      """
    Then the reply equals:
      """
      "[]"
      """
    When I execute:
      """
      ["JSON.GET","doc","$.obj.c"]
      """
    Then the reply equals:
      """
      "[3]"
      """
    When I execute:
      """
      ["JSON.DEL","doc","$.obj.b"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["JSON.FORGET","doc","$.obj.c"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["JSON.CLEAR","doc","$.a"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["JSON.CLEAR","doc","$.n"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["JSON.GET","doc","$.a"]
      """
    Then the reply equals:
      """
      "[[]]"
      """
    When I execute:
      """
      ["JSON.GET","doc","$.n"]
      """
    Then the reply equals:
      """
      "[0]"
      """
    When I execute:
      """
      ["JSON.OBJLEN","doc","$.obj"]
      """
    Then the reply equals:
      """
      [0]
      """
  @command_JSON_MSET @command_JSON_MGET
  Scenario: Multi-document assignment is atomic and multi-get preserves missing keys
    Given a fresh ffkv server
    When I execute:
      """
      ["JSON.MSET","a","$","{\"v\":1}","b","$","{\"v\":2}"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["JSON.MGET","b","absent","a","$.v"]
      """
    Then the reply equals:
      """
      ["[2]",null,"[1]"]
      """
    When I execute:
      """
      ["JSON.MSET","a","$.v","99","missing","$.child","1"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["JSON.MGET","a","b","$.v"]
      """
    Then the reply equals:
      """
      ["[1]","[2]"]
      """
  @command_JSON_DEBUG @command_JSON_DEBUG_MEMORY
  Scenario: Debug memory distinguishes present data from unmatched paths without pinning allocator sizes
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["JSON.SET","doc","$","{\"payload\":[1,2,3]}"]]
      """
    When I execute:
      """
      ["JSON.DEBUG","MEMORY","doc"]
      """
    Then the reply is an integer greater than 0
    When I execute:
      """
      ["JSON.DEBUG","MEMORY","doc","$.missing"]
      """
    Then the reply equals:
      """
      []
      """
    When I execute:
      """
      ["JSON.GET","doc","$.payload"]
      """
    Then the reply equals:
      """
      "[[1,2,3]]"
      """

  @command_JSON_GET @command_JSON_MGET @command_JSON_NUMINCRBY @command_JSON_DEL
  Scenario: Repeated read selectors preserve their sequence while mutations use unique targets
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["JSON.SET","j","$","[10,20]"]]
      """
    When I execute:
      """
      ["JSON.GET","j","$[1,0,1]"]
      """
    Then the reply equals:
      """
      "[20,10,20]"
      """
    When I execute:
      """
      ["JSON.MGET","j","missing","j","$[1,0,1]"]
      """
    Then the reply equals:
      """
      ["[20,10,20]",null,"[20,10,20]"]
      """
    When I execute:
      """
      ["JSON.GET","j","count($[1,0,1])"]
      """
    Then the reply equals:
      """
      "[3]"
      """
    When I execute:
      """
      ["JSON.NUMINCRBY","j","$[1,0,1]",1]
      """
    Then the reply equals:
      """
      "[21,11]"
      """
    When ffkv restarts after a crash
    When I execute:
      """
      ["JSON.GET","j","$[1,0,1]"]
      """
    Then the reply equals:
      """
      "[21,11,21]"
      """
    When I execute:
      """
      ["JSON.DEL","j","$[0,0]"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["JSON.GET","j","$"]
      """
    Then the reply equals:
      """
      "[[21]]"
      """
