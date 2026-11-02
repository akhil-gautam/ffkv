Feature: Probabilistic filters reuse existing storage and creation settings

  @command_BF_RESERVE @command_BF_INSERT @command_BF_EXISTS @command_CF_RESERVE @command_CF_INSERT @command_CF_EXISTS
  Scenario: Insert ignores creation allocation budgets for existing filters
    Given a fresh ffkv server
    And client "v3" uses RESP 3
    And the following commands succeed:
      """
      [["BF.RESERVE","bf",0.01,10],["CF.RESERVE","cf",10]]
      """
    When client "v3" executes:
      """
      ["BF.INSERT","bf","CAPACITY",1073741824,"ITEMS","x"]
      """
    Then the reply equals:
      """
      [true]
      """
    When client "v3" executes:
      """
      ["BF.EXISTS","bf","x"]
      """
    Then the reply equals:
      """
      true
      """
    When client "v3" executes:
      """
      ["CF.INSERT","cf","CAPACITY",1073741824,"ITEMS","x"]
      """
    Then the reply equals:
      """
      [true]
      """
    When client "v3" executes:
      """
      ["CF.EXISTS","cf","x"]
      """
    Then the reply equals:
      """
      true
      """
    When client "v3" executes:
      """
      ["BF.INSERT","missing-bf","CAPACITY",1073741824,"ITEMS","x"]
      """
    Then the reply is an error with code "ERR"
    When client "v3" executes:
      """
      ["CF.INSERT","missing-cf","CAPACITY",1073741824,"ITEMS","x"]
      """
    Then the reply is an error with code "ERR"
    When client "v3" executes:
      """
      ["EXISTS","missing-bf","missing-cf"]
      """
    Then the reply equals:
      """
      0
      """
    When ffkv restarts after a crash
    When client "v3" executes:
      """
      ["BF.EXISTS","bf","x"]
      """
    Then the reply equals:
      """
      true
      """
    When client "v3" executes:
      """
      ["CF.EXISTS","cf","x"]
      """
    Then the reply equals:
      """
      true
      """

  @command_CF_RESERVE @command_CF_INSERT @command_CF_INFO @command_CF_COUNT @command_CF_DEL
  Scenario: Cuckoo insertion reuses earlier parts and preserves duplicate counts after replay
    Given a fresh ffkv server
    And client "v3" uses RESP 3
    And the following commands succeed:
      """
      [["CF.RESERVE","cf",4,"BUCKETSIZE",1,"EXPANSION",1],["CF.INSERT","cf","ITEMS","","","",""]]
      """
    When client "v3" executes:
      """
      ["CF.INSERT","cf","ITEMS","hello","hello"]
      """
    Then the reply equals:
      """
      [true,true]
      """
    When client "v3" executes:
      """
      ["CF.INFO","cf"]
      """
    Then the reply contains:
      """
      {"Size":8,"Number of filters":2,"Number of items inserted":6}
      """
    When client "v3" executes:
      """
      ["CF.COUNT","cf",""]
      """
    Then the reply equals:
      """
      4
      """
    When client "v3" executes:
      """
      ["CF.COUNT","cf","hello"]
      """
    Then the reply equals:
      """
      2
      """
    When ffkv restarts after a crash
    When client "v3" executes:
      """
      ["CF.INFO","cf"]
      """
    Then the reply contains:
      """
      {"Size":8,"Number of filters":2,"Number of items inserted":6}
      """
    When client "v3" executes:
      """
      ["CF.COUNT","cf",""]
      """
    Then the reply equals:
      """
      4
      """
    When client "v3" executes:
      """
      ["CF.COUNT","cf","hello"]
      """
    Then the reply equals:
      """
      2
      """
    When client "v3" executes:
      """
      ["CF.DEL","cf","hello"]
      """
    Then the reply equals:
      """
      true
      """
    When client "v3" executes:
      """
      ["CF.COUNT","cf","hello"]
      """
    Then the reply equals:
      """
      1
      """
    When client "v3" executes:
      """
      ["CF.COUNT","cf",""]
      """
    Then the reply equals:
      """
      4
      """
