# Contracts: https://redis.io/docs/latest/commands/arset/; https://redis.io/docs/latest/commands/arinsert/; https://redis.io/docs/latest/commands/arop/; https://redis.io/docs/latest/commands/arring/
Feature: Sparse indices differ from occupied values and insertion cursors

  @command_ARSET @command_ARMSET @command_ARGET @command_ARMGET @command_ARGETRANGE @command_ARLEN @command_ARCOUNT @command_ARINFO
  Scenario: Sparse assignment exposes holes and distinguishes length from occupied count
    Given a fresh ffkv server
    And client "array3" uses RESP 3
    When I execute:
      """
      ["ARSET","slots",2,"two","three"]
      """
    Then the reply equals:
      """
      2
      """
    When I execute:
      """
      ["ARMSET","slots",5,"five",8,"eight"]
      """
    Then the reply equals:
      """
      2
      """
    When I execute:
      """
      ["ARGET","slots",2]
      """
    Then the reply equals:
      """
      "two"
      """
    When I execute:
      """
      ["ARGET","slots",4]
      """
    Then the reply is null
    When I execute:
      """
      ["ARMGET","slots",8,0,3,5]
      """
    Then the reply equals:
      """
      ["eight",null,"three","five"]
      """
    When I execute:
      """
      ["ARGETRANGE","slots",5,2]
      """
    Then the reply equals:
      """
      ["five",null,"three","two"]
      """
    When I execute:
      """
      ["ARLEN","slots"]
      """
    Then the reply equals:
      """
      9
      """
    When I execute:
      """
      ["ARCOUNT","slots"]
      """
    Then the reply equals:
      """
      4
      """
    When client "array3" executes:
      """
      ["ARINFO","slots"]
      """
    Then the reply contains:
      """
      {"count":4,"len":9}
      """
    When ffkv restarts after a crash
    When I execute:
      """
      ["ARGETRANGE","slots",2,5]
      """
    Then the reply equals:
      """
      ["two","three",null,"five"]
      """
  @command_ARDEL @command_ARDELRANGE
  Scenario: Deleting repeated indices and overlapping ranges leaves holes without shifting survivors
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["ARSET","slots",0,"a","b","c","d","e","f"]]
      """
    When I execute:
      """
      ["ARDEL","slots",1,1,99]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["ARDELRANGE","slots",4,2,3,4]
      """
    Then the reply equals:
      """
      3
      """
    When I execute:
      """
      ["ARGETRANGE","slots",0,5]
      """
    Then the reply equals:
      """
      ["a",null,null,null,null,"f"]
      """
    When I execute:
      """
      ["ARCOUNT","slots"]
      """
    Then the reply equals:
      """
      2
      """
    When I execute:
      """
      ["ARLEN","slots"]
      """
    Then the reply equals:
      """
      6
      """
  @command_ARINSERT @command_ARNEXT @command_ARSEEK
  Scenario: Seeking changes the insertion cursor independently of sparse highest index
    Given a fresh ffkv server
    When I execute:
      """
      ["ARINSERT","log","first","second"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["ARNEXT","log"]
      """
    Then the reply equals:
      """
      2
      """
    When I execute:
      """
      ["ARSET","log",9,"sparse"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["ARNEXT","log"]
      """
    Then the reply equals:
      """
      2
      """
    When I execute:
      """
      ["ARSEEK","log",5]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["ARINSERT","log","at-five"]
      """
    Then the reply equals:
      """
      5
      """
    When I execute:
      """
      ["ARNEXT","log"]
      """
    Then the reply equals:
      """
      6
      """
    When I execute:
      """
      ["ARGETRANGE","log",4,6]
      """
    Then the reply equals:
      """
      [null,"at-five",null]
      """
    When I execute:
      """
      ["ARLEN","log"]
      """
    Then the reply equals:
      """
      10
      """
    When ffkv restarts after a crash
    When I execute:
      """
      ["ARNEXT","log"]
      """
    Then the reply equals:
      """
      6
      """
  @command_ARRING @command_ARLASTITEMS
  Scenario: Ring wrapping overwrites the oldest slot while latest items retain chronological order
    Given a fresh ffkv server
    When I execute:
      """
      ["ARRING","readings",3,"v0","v1","v2"]
      """
    Then the reply equals:
      """
      2
      """
    When I execute:
      """
      ["ARRING","readings",3,"v3"]
      """
    Then the reply equals:
      """
      0
      """
    When I execute:
      """
      ["ARGETRANGE","readings",0,2]
      """
    Then the reply equals:
      """
      ["v3","v1","v2"]
      """
    When I execute:
      """
      ["ARLASTITEMS","readings",3]
      """
    Then the reply equals:
      """
      ["v1","v2","v3"]
      """
    When I execute:
      """
      ["ARLASTITEMS","readings",2,"REV"]
      """
    Then the reply equals:
      """
      ["v3","v2"]
      """
    When I execute:
      """
      ["ARCOUNT","readings"]
      """
    Then the reply equals:
      """
      3
      """
    When ffkv restarts after a crash
    When I execute:
      """
      ["ARLASTITEMS","readings",3]
      """
    Then the reply equals:
      """
      ["v1","v2","v3"]
      """
  @command_ARSCAN @command_ARGREP
  Scenario: Scans and text predicates return existing indices in the requested direction
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["ARMSET","words",0,"Apple",3,"apricot",7,"pear"]]
      """
    When I execute:
      """
      ["ARSCAN","words",7,0,"LIMIT",2]
      """
    Then the reply equals:
      """
      [[7,"pear"],[3,"apricot"]]
      """
    When I execute:
      """
      ["ARGREP","words",0,7,"GLOB","ap*","NOCASE","WITHVALUES"]
      """
    Then the reply equals:
      """
      [[0,"Apple"],[3,"apricot"]]
      """
    When I execute:
      """
      ["ARGREP","words",7,0,"EXACT","pear"]
      """
    Then the reply equals:
      """
      [7]
      """
    When I execute:
      """
      ["ARGET","words",3]
      """
    Then the reply equals:
      """
      "apricot"
      """
  @command_AROP
  Scenario: Numeric and bitwise aggregation ignores holes and counts used values separately
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["ARMSET","numbers",0,"10",3,"20",7,"30"]]
      """
    When I execute:
      """
      ["AROP","numbers",7,0,"SUM"]
      """
    Then the reply equals:
      """
      "60"
      """
    When I execute:
      """
      ["AROP","numbers",0,7,"MIN"]
      """
    Then the reply equals:
      """
      "10"
      """
    When I execute:
      """
      ["AROP","numbers",0,7,"MAX"]
      """
    Then the reply equals:
      """
      "30"
      """
    When I execute:
      """
      ["AROP","numbers",0,7,"USED"]
      """
    Then the reply equals:
      """
      3
      """
    When I execute:
      """
      ["AROP","numbers",0,7,"MATCH","20"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["AROP","numbers",1,2,"SUM"]
      """
    Then the reply is null
    And the following commands succeed:
      """
      [["ARSET","flags",0,"255","15","240"]]
      """
    When I execute:
      """
      ["AROP","flags",0,2,"AND"]
      """
    Then the reply equals:
      """
      0
      """
    When I execute:
      """
      ["AROP","flags",0,2,"OR"]
      """
    Then the reply equals:
      """
      255
      """
    When I execute:
      """
      ["AROP","flags",0,2,"XOR"]
      """
    Then the reply equals:
      """
      0
      """
  
  Scenario: Unsigned index overflow rejects the whole contiguous write without replacing a value
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["ARSET","wide",18446744073709551614,"tail"]]
      """
    When I execute:
      """
      ["ARLEN","wide"]
      """
    Then the reply equals:
      """
      "18446744073709551615"
      """
    When I execute:
      """
      ["ARSET","wide",18446744073709551614,"replace","overflow"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["ARGET","wide",18446744073709551614]
      """
    Then the reply equals:
      """
      "tail"
      """
    When I execute:
      """
      ["ARCOUNT","wide"]
      """
    Then the reply equals:
      """
      1
      """

  @command_EVAL @command_ARLEN
  Scenario Outline: Lua preserves arbitrary decimal replies and sparse cardinality beyond signed integers
    Given a fresh ffkv server
    And client "big" uses RESP <version>
    When client "big" executes:
      """
      ["ARSET","wide",18446744073709551614,"tail"]
      """
    Then the reply equals:
      """
      1
      """
    When client "big" executes:
      """
      ["EVAL","redis.setresp(3); local n=redis.call('ARLEN',KEYS[1]); return {n.big_number,n}",1,"wide"]
      """
    Then the reply equals:
      """
      ["18446744073709551615",<cardinality>]
      """
    When client "big" executes:
      """
      ["EVAL","return {big_number='-340282366920938463463374607431768211456'}",0]
      """
    Then the reply equals:
      """
      <signed>
      """
    When client "big" executes:
      """
      ["EVAL","return {big_number='1\\r\\n+injected'}",0]
      """
    Then the reply is an error with code "ERR"
    When client "big" executes:
      """
      ["PING"]
      """
    Then the reply equals:
      """
      "PONG"
      """

    Examples:
      | version | cardinality                                                  | signed                                                                                |
      | 2       | "18446744073709551615"                                        | "-340282366920938463463374607431768211456"                                              |
      | 3       | {"type":"big-number","value":"18446744073709551615"}           | {"type":"big-number","value":"-340282366920938463463374607431768211456"}                 |
