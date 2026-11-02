# Contracts: https://redis.io/docs/latest/develop/using-commands/transactions/
Feature: Persistent connections isolate optimistic transactions

  @command_MULTI @command_EXEC
  Scenario: Queued writes remain invisible until EXEC and commit in order
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["SET","balance","10"]]
      """
    When client "buyer" executes:
      """
      ["MULTI"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "buyer" executes:
      """
      ["INCRBY","balance",5]
      """
    Then the reply equals:
      """
      "QUEUED"
      """
    When client "buyer" executes:
      """
      ["GET","balance"]
      """
    Then the reply equals:
      """
      "QUEUED"
      """
    When client "observer" executes:
      """
      ["GET","balance"]
      """
    Then the reply equals:
      """
      "10"
      """
    When client "buyer" executes:
      """
      ["EXEC"]
      """
    Then the reply equals:
      """
      [15,"15"]
      """
    When ffkv restarts after a crash
    When I execute:
      """
      ["GET","balance"]
      """
    Then the reply equals:
      """
      "15"
      """
  @command_WATCH
  Scenario: A competing write aborts WATCH without applying queued changes
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["SET","stock","1"]]
      """
    When client "buyer" executes:
      """
      ["WATCH","stock"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "seller" executes:
      """
      ["SET","stock","0"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "buyer" executes:
      """
      ["MULTI"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "buyer" executes:
      """
      ["SET","receipt","paid"]
      """
    Then the reply equals:
      """
      "QUEUED"
      """
    When client "buyer" executes:
      """
      ["EXEC"]
      """
    Then the reply is null
    When client "seller" executes:
      """
      ["MGET","stock","receipt"]
      """
    Then the reply equals:
      """
      ["0",null]
      """
  @command_UNWATCH
  Scenario: UNWATCH permits a later transaction after the watched value changes
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["SET","stock","1"]]
      """
    When client "buyer" executes:
      """
      ["WATCH","stock"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "buyer" executes:
      """
      ["UNWATCH"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "seller" executes:
      """
      ["SET","stock","2"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "buyer" executes:
      """
      ["MULTI"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "buyer" executes:
      """
      ["GET","stock"]
      """
    Then the reply equals:
      """
      "QUEUED"
      """
    When client "buyer" executes:
      """
      ["EXEC"]
      """
    Then the reply equals:
      """
      ["2"]
      """
  @command_DISCARD
  Scenario: DISCARD removes the queue and watches but not committed values
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["SET","stock","1"]]
      """
    When client "buyer" executes:
      """
      ["WATCH","stock"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "buyer" executes:
      """
      ["MULTI"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "buyer" executes:
      """
      ["SET","stock","discarded"]
      """
    Then the reply equals:
      """
      "QUEUED"
      """
    When client "buyer" executes:
      """
      ["DISCARD"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "observer" executes:
      """
      ["GET","stock"]
      """
    Then the reply equals:
      """
      "1"
      """
    When client "observer" executes:
      """
      ["SET","stock","2"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "buyer" executes:
      """
      ["MULTI"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "buyer" executes:
      """
      ["GET","stock"]
      """
    Then the reply equals:
      """
      "QUEUED"
      """
    When client "buyer" executes:
      """
      ["EXEC"]
      """
    Then the reply equals:
      """
      ["2"]
      """
  
  Scenario: Invalid queued arity aborts the entire transaction without mutation
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["SET","stock","1"]]
      """
    When client "buyer" executes:
      """
      ["MULTI"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "buyer" executes:
      """
      ["SET","stock","discarded"]
      """
    Then the reply equals:
      """
      "QUEUED"
      """
    When client "buyer" executes:
      """
      ["SET","stock"]
      """
    Then the reply is an error with code "ERR"
    When client "buyer" executes:
      """
      ["EXEC"]
      """
    Then the reply is an error with code "EXECABORT"
    When I execute:
      """
      ["GET","stock"]
      """
    Then the reply equals:
      """
      "1"
      """

  @command_MULTI @command_EXEC @command_EVAL @command_EVALSHA @command_FCALL
  Scenario Outline: Invalid counted script keys fail during EXEC without discarding adjacent writes
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["MULTI"],["SET","before","retained"]]
      """
    When I execute:
      """
      ["<command>","return 1","<count>"]
      """
    Then the reply equals:
      """
      "QUEUED"
      """
    When I execute:
      """
      ["SET","after","retained"]
      """
    Then the reply equals:
      """
      "QUEUED"
      """
    When I execute:
      """
      ["EXEC"]
      """
    Then the reply equals:
      """
      ["OK",{"type":"error","value":"<error>"},"OK"]
      """
    When I execute:
      """
      ["MGET","before","after"]
      """
    Then the reply equals:
      """
      ["retained","retained"]
      """

    Examples:
      | command | count | error                                             |
      | EVAL    | -1    | ERR Number of keys can't be negative               |
      | EVAL    | 1     | ERR Number of keys can't be greater than number of args |
      | EVAL    | bad   | ERR value is not an integer or out of range        |
      | EVALSHA | -1    | ERR Number of keys can't be negative               |
      | FCALL   | -1    | ERR Number of keys can't be negative               |

  @command_MULTI @command_EXEC @command_EVAL
  Scenario: Invalid script arity still poisons the transaction at queue admission
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["MULTI"],["SET","before","discarded"]]
      """
    When I execute:
      """
      ["EVAL","return 1"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["EXEC"]
      """
    Then the reply is an error with code "EXECABORT"
    When I execute:
      """
      ["GET","before"]
      """
    Then the reply is null
