# Contracts: https://redis.io/docs/latest/commands/vemb/; https://redis.io/docs/latest/commands/vsim/; https://redis.io/docs/latest/commands/vlinks/
# RAW tags follow Redis 8.10 GA vectorSetGetQuantName: https://github.com/redis/redis/blob/5279a8d44818a5ca51e9abb91a9b8ce481d3c88b/modules/vector-sets/vset.c
Feature: Vector membership binary embeddings and attribute-filtered similarity

  @command_VADD @command_VDIM @command_VCARD @command_VEMB @command_VISMEMBER
  Scenario: Binary insertion preserves numeric dimensions embeddings and raw float data
    Given a fresh ffkv server
    And client "v3" uses RESP 3
    When client "v3" executes:
      """
      ["VADD","directions","FP32",{"base64":"AACAPwAAAAAAAAAA"},"east","NOQUANT"]
      """
    Then the reply equals:
      """
      true
      """
    When client "v3" executes:
      """
      ["VDIM","directions"]
      """
    Then the reply equals:
      """
      3
      """
    When client "v3" executes:
      """
      ["VCARD","directions"]
      """
    Then the reply equals:
      """
      1
      """
    When client "v3" executes:
      """
      ["VISMEMBER","directions","east"]
      """
    Then the reply equals:
      """
      true
      """
    When client "v3" executes:
      """
      ["VISMEMBER","directions","absent"]
      """
    Then the reply equals:
      """
      false
      """
    When client "v3" executes:
      """
      ["VEMB","directions","east"]
      """
    Then the reply equals:
      """
      [{"$approx":1},{"$approx":0},{"$approx":0}]
      """
    When client "v3" executes:
      """
      ["VEMB","directions","east","RAW"]
      """
    Then the reply equals:
      """
      ["f32",{"type":"bytes","base64":"AACAPwAAAAAAAAAA"},{"$approx":1}]
      """
    When client "v3" executes:
      """
      ["VADD","directions","VALUES",2,1,0,"bad"]
      """
    Then the reply is an error with code "ERR"
    When client "v3" executes:
      """
      ["VCARD","directions"]
      """
    Then the reply equals:
      """
      1
      """
    When ffkv restarts after a crash
    When client "v3" executes:
      """
      ["VEMB","directions","east"]
      """
    Then the reply equals:
      """
      [{"$approx":1},{"$approx":0},{"$approx":0}]
      """
  @command_VSIM @command_VSETATTR @command_VGETATTR
  Scenario: Similarity scores and JSON attributes select semantic neighbors
    Given a fresh ffkv server
    And client "v3" uses RESP 3
    And the following commands succeed:
      """
      [["VADD","directions","VALUES",3,1,0,0,"east","NOQUANT","SETATTR","{\"zone\":\"east\"}"],["VADD","directions","VALUES",3,0,1,0,"north","NOQUANT","SETATTR","{\"zone\":\"north\"}"]]
      """
    When client "v3" executes:
      """
      ["VSIM","directions","ELE","east","COUNT",1,"WITHSCORES","WITHATTRIBS"]
      """
    Then the reply equals:
      """
      {"east":[{"$approx":1},"{\"zone\":\"east\"}"]}
      """
    When client "v3" executes:
      """
      ["VSIM","directions","VALUES",3,1,0,0,"FILTER",".zone == \"north\"","COUNT",1,"FILTER-EF",0]
      """
    Then the reply equals:
      """
      ["north"]
      """
    When client "v3" executes:
      """
      ["VSETATTR","directions","north","{\"zone\":\"east\"}"]
      """
    Then the reply equals:
      """
      true
      """
    When client "v3" executes:
      """
      ["VGETATTR","directions","north"]
      """
    Then the reply equals:
      """
      "{\"zone\":\"east\"}"
      """
    When client "v3" executes:
      """
      ["VSIM","directions","ELE","east","FILTER",".zone == \"north\"","COUNT",2]
      """
    Then the reply equals:
      """
      []
      """
    When client "v3" executes:
      """
      ["VSIM","directions","ELE","east","FILTER","é"]
      """
    Then the reply is an error with code "ERR"
    When client "v3" executes:
      """
      ["VGETATTR","directions","north"]
      """
    Then the reply equals:
      """
      "{\"zone\":\"east\"}"
      """
  @command_VRANGE @command_VREM @command_VRANDMEMBER
  Scenario: Lexical range deletion and random selection operate on actual remaining members
    Given a fresh ffkv server
    And client "v3" uses RESP 3
    And the following commands succeed:
      """
      [["VADD","directions","VALUES",2,1,0,"alpha","NOQUANT"],["VADD","directions","VALUES",2,0,1,"beta","NOQUANT"],["VADD","directions","VALUES",2,-1,0,"gamma","NOQUANT"]]
      """
    When client "v3" executes:
      """
      ["VRANGE","directions","[alpha","[beta"]
      """
    Then the reply equals:
      """
      ["alpha","beta"]
      """
    When client "v3" executes:
      """
      ["VREM","directions","alpha"]
      """
    Then the reply equals:
      """
      true
      """
    When client "v3" executes:
      """
      ["VREM","directions","gamma"]
      """
    Then the reply equals:
      """
      true
      """
    When client "v3" executes:
      """
      ["VRANDMEMBER","directions"]
      """
    Then the reply equals:
      """
      "beta"
      """
    When client "v3" executes:
      """
      ["VRANDMEMBER","directions",-3]
      """
    Then the reply equals:
      """
      ["beta","beta","beta"]
      """
    When client "v3" executes:
      """
      ["VCARD","directions"]
      """
    Then the reply equals:
      """
      1
      """
    When client "v3" executes:
      """
      ["VREM","directions","beta"]
      """
    Then the reply equals:
      """
      true
      """
    When client "v3" executes:
      """
      ["VISMEMBER","directions","beta"]
      """
    Then the reply equals:
      """
      false
      """
  @command_VLINKS @command_VINFO
  Scenario: Graph links and metadata describe inserted vectors rather than physical memory sizes
    Given a fresh ffkv server
    And client "v3" uses RESP 3
    And the following commands succeed:
      """
      [["VADD","directions","VALUES",2,1,0,"east","NOQUANT"],["VADD","directions","VALUES",2,0,1,"north","NOQUANT"]]
      """
    When client "v3" executes:
      """
      ["VLINKS","directions","east"]
      """
    Then the reply contains:
      """
      [["north"]]
      """
    When client "v3" executes:
      """
      ["VINFO","directions"]
      """
    Then the reply contains:
      """
      {"vector-dim":2,"size":2}
      """

  @command_VADD @command_VSIM
  Scenario: Null filters distinguish numeric zero without removing numeric coercion
    Given a fresh ffkv server
    And client "v3" uses RESP 3
    And the following commands succeed:
      """
      [["VADD","v","VALUES",2,1,0,"item","NOQUANT","SETATTR","{\"x\":0}"]]
      """
    When client "v3" executes:
      """
      ["VSIM","v","ELE","item","FILTER",".x == null","TRUTH","NOTHREAD"]
      """
    Then the reply equals:
      """
      []
      """
    When client "v3" executes:
      """
      ["VSIM","v","ELE","item","FILTER",".x != null","TRUTH","NOTHREAD"]
      """
    Then the reply equals:
      """
      ["item"]
      """
    When client "v3" executes:
      """
      ["VSIM","v","ELE","item","FILTER",".x in [false, '0']","TRUTH","NOTHREAD"]
      """
    Then the reply equals:
      """
      ["item"]
      """
