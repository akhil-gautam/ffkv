# Contracts: https://redis.io/docs/latest/commands/ts.range/; https://redis.io/docs/latest/commands/ts.mget/; https://redis.io/docs/latest/commands/ts.info/
Feature: Timestamped samples retention compaction and multi-series queries

  @command_TS_CREATE @command_TS_ADD @command_TS_ALTER @command_TS_GET @command_TS_INFO
  Scenario: Explicit retention and duplicate policies govern updates and pruning
    Given a fresh ffkv server
    And client "ts3" uses RESP 3
    When client "ts3" executes:
      """
      ["TS.CREATE","sensor","RETENTION",100,"DUPLICATE_POLICY","BLOCK","LABELS","room","study"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "ts3" executes:
      """
      ["TS.ADD","sensor",1000,2]
      """
    Then the reply equals:
      """
      1000
      """
    When client "ts3" executes:
      """
      ["TS.ADD","sensor",1000,9]
      """
    Then the reply is an error
    When client "ts3" executes:
      """
      ["TS.GET","sensor"]
      """
    Then the reply equals:
      """
      [1000,{"$approx":2}]
      """
    When client "ts3" executes:
      """
      ["TS.ALTER","sensor","DUPLICATE_POLICY","SUM","LABELS","room","kitchen"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "ts3" executes:
      """
      ["TS.ADD","sensor",1000,3]
      """
    Then the reply equals:
      """
      1000
      """
    When client "ts3" executes:
      """
      ["TS.GET","sensor"]
      """
    Then the reply equals:
      """
      [1000,{"$approx":5}]
      """
    When client "ts3" executes:
      """
      ["TS.ADD","sensor",1101,8]
      """
    Then the reply equals:
      """
      1101
      """
    When client "ts3" executes:
      """
      ["TS.RANGE","sensor","-","+"]
      """
    Then the reply equals:
      """
      [[1101,{"$approx":8}]]
      """
    When client "ts3" executes:
      """
      ["TS.ADD","sensor",999,1]
      """
    Then the reply is an error
    When client "ts3" executes:
      """
      ["TS.INFO","sensor"]
      """
    Then the reply contains:
      """
      {"totalSamples":1,"firstTimestamp":1101,"lastTimestamp":1101,"retentionTime":100,"duplicatePolicy":"sum"}
      """
    When client "ts3" executes:
      """
      ["TS.QUERYINDEX","room=study"]
      """
    Then the reply equals:
      """
      {"type":"set","values":[]}
      """
    When client "ts3" executes:
      """
      ["TS.QUERYINDEX","room=kitchen"]
      """
    Then the reply equals:
      """
      {"type":"set","values":["sensor"]}
      """
    When ffkv restarts after a crash
    When client "ts3" executes:
      """
      ["TS.GET","sensor"]
      """
    Then the reply equals:
      """
      [1101,{"$approx":8}]
      """
  @command_TS_INCRBY @command_TS_DECRBY @command_TS_DEL
  Scenario: Counter deltas and inclusive deletion retain the correct remaining samples
    Given a fresh ffkv server
    And client "ts3" uses RESP 3
    And the following commands succeed:
      """
      [["TS.CREATE","counter"],["TS.ADD","counter",10,5]]
      """
    When client "ts3" executes:
      """
      ["TS.INCRBY","counter",3,"TIMESTAMP",20]
      """
    Then the reply equals:
      """
      20
      """
    When client "ts3" executes:
      """
      ["TS.DECRBY","counter",2,"TIMESTAMP",20]
      """
    Then the reply equals:
      """
      20
      """
    When client "ts3" executes:
      """
      ["TS.GET","counter"]
      """
    Then the reply equals:
      """
      [20,{"$approx":6}]
      """
    When client "ts3" executes:
      """
      ["TS.INCRBY","counter",4,"TIMESTAMP",30]
      """
    Then the reply equals:
      """
      30
      """
    When client "ts3" executes:
      """
      ["TS.DEL","counter",10,20]
      """
    Then the reply equals:
      """
      2
      """
    When client "ts3" executes:
      """
      ["TS.RANGE","counter","-","+"]
      """
    Then the reply equals:
      """
      [[30,{"$approx":10}]]
      """
  @command_TS_RANGE @command_TS_REVRANGE @command_TS_MADD
  Scenario: Range order filters and multi-aggregator statistics follow sample values
    Given a fresh ffkv server
    And client "ts3" uses RESP 3
    And the following commands succeed:
      """
      [["TS.CREATE","sensor"]]
      """
    When client "ts3" executes:
      """
      ["TS.MADD","sensor",10,2,"sensor",20,4,"sensor",30,6,"sensor",110,10]
      """
    Then the reply equals:
      """
      [10,20,30,110]
      """
    When client "ts3" executes:
      """
      ["TS.RANGE","sensor","-","+","FILTER_BY_VALUE",3,9]
      """
    Then the reply equals:
      """
      [[20,{"$approx":4}],[30,{"$approx":6}]]
      """
    When client "ts3" executes:
      """
      ["TS.REVRANGE","sensor","-","+","COUNT",2]
      """
    Then the reply equals:
      """
      [[110,{"$approx":10}],[30,{"$approx":6}]]
      """
    When client "ts3" executes:
      """
      ["TS.RANGE","sensor",0,99,"AGGREGATION","min,avg,max,sum,range,count,first,last,var.p,var.s,std.p,std.s",100]
      """
    Then the reply equals:
      """
      [[0,{"$approx":2},{"$approx":4},{"$approx":6},{"$approx":12},{"$approx":4},{"$approx":3},{"$approx":2},{"$approx":6},{"$approx":2.6666666666666665},{"$approx":4},{"$approx":1.632993161855452},{"$approx":2}]]
      """
  
  Scenario: NaN samples remain visible while aggregation counts distinguish missing numbers
    Given a fresh ffkv server
    And client "ts3" uses RESP 3
    And the following commands succeed:
      """
      [["TS.CREATE","sensor"],["TS.ADD","sensor",10,2],["TS.ADD","sensor",20,"nan"],["TS.ADD","sensor",210,6]]
      """
    When client "ts3" executes:
      """
      ["TS.RANGE","sensor",10,20]
      """
    Then the reply equals:
      """
      [[10,{"$approx":2}],[20,{"type":"double","value":"nan"}]]
      """
    When client "ts3" executes:
      """
      ["TS.RANGE","sensor",0,99,"AGGREGATION","count,countNaN,countAll,sum",100]
      """
    Then the reply equals:
      """
      [[0,{"$approx":1},{"$approx":1},{"$approx":2},{"$approx":2}]]
      """
    When client "ts3" executes:
      """
      ["TS.RANGE","sensor",0,299,"AGGREGATION","sum",100,"EMPTY"]
      """
    Then the reply equals:
      """
      [[0,{"$approx":2}],[100,{"$approx":0}],[200,{"$approx":6}]]
      """
  @command_TS_CREATERULE @command_TS_DELETERULE
  Scenario: Compaction exposes closed and open buckets and deleting its rule stops future output
    Given a fresh ffkv server
    And client "ts3" uses RESP 3
    And the following commands succeed:
      """
      [["TS.CREATE","raw"],["TS.CREATE","average"]]
      """
    When client "ts3" executes:
      """
      ["TS.CREATERULE","raw","average","AGGREGATION","avg",100]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "ts3" executes:
      """
      ["TS.MADD","raw",20,2,"raw",80,4,"raw",150,8]
      """
    Then the reply equals:
      """
      [20,80,150]
      """
    When client "ts3" executes:
      """
      ["TS.RANGE","average","-","+"]
      """
    Then the reply equals:
      """
      [[0,{"$approx":3}]]
      """
    When client "ts3" executes:
      """
      ["TS.GET","average","LATEST"]
      """
    Then the reply equals:
      """
      [100,{"$approx":8}]
      """
    When ffkv restarts after a crash
    When client "ts3" executes:
      """
      ["TS.GET","average","LATEST"]
      """
    Then the reply equals:
      """
      [100,{"$approx":8}]
      """
    When client "ts3" executes:
      """
      ["TS.DELETERULE","raw","average"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "ts3" executes:
      """
      ["TS.ADD","raw",250,20]
      """
    Then the reply equals:
      """
      250
      """
    When client "ts3" executes:
      """
      ["TS.RANGE","average","-","+"]
      """
    Then the reply equals:
      """
      [[0,{"$approx":3}]]
      """
  @command_TS_QUERYINDEX @command_TS_QUERYLABELS @command_TS_MGET @command_TS_MRANGE @command_TS_MREVRANGE
  Scenario: Label indexes and filtered multi-series queries reflect changed metadata
    Given a fresh ffkv server
    And client "ts3" uses RESP 3
    And the following commands succeed:
      """
      [["TS.CREATE","east","LABELS","zone","east","kind","temp"],["TS.CREATE","west","LABELS","zone","west","kind","temp"],["TS.MADD","east",10,2,"east",20,4,"west",10,7,"west",30,9]]
      """
    When client "ts3" executes:
      """
      ["TS.QUERYINDEX","kind=temp","zone=east"]
      """
    Then the reply equals:
      """
      {"type":"set","values":["east"]}
      """
    When client "ts3" executes:
      """
      ["TS.QUERYLABELS","VALUES","zone","FILTER","kind=temp"]
      """
    Then the reply equals:
      """
      {"$one_of":[{"type":"set","values":["east","west"]},{"type":"set","values":["west","east"]}]}
      """
    And the reply has 2 items
    When client "ts3" executes:
      """
      ["TS.QUERYLABELS","LABELS","FILTER","zone=east"]
      """
    Then the reply equals:
      """
      {"$one_of":[{"type":"set","values":["kind","zone"]},{"type":"set","values":["zone","kind"]}]}
      """
    And the reply has 2 items
    When client "ts3" executes:
      """
      ["TS.MGET","SELECTED_LABELS","zone","FILTER","kind=temp","zone=east"]
      """
    Then the reply equals:
      """
      {"east":[{"zone":"east"},[20,{"$approx":4}]]}
      """
    When I execute:
      """
      ["TS.MRANGE",0,30,"SELECTED_LABELS","zone","FILTER","kind=temp","zone=east"]
      """
    Then the reply equals:
      """
      [["east",[["zone","east"]],[[10,"2"],[20,"4"]]]]
      """
    When I execute:
      """
      ["TS.MREVRANGE",0,30,"SELECTED_LABELS","zone","FILTER","kind=temp","zone=west"]
      """
    Then the reply equals:
      """
      [["west",[["zone","west"]],[[30,"9"],[10,"7"]]]]
      """
  @command_TS_NRANGE @command_TS_NREVRANGE
  Scenario: Named-series queries group unequal timestamps without fabricating absent samples
    Given a fresh ffkv server
    And client "ts3" uses RESP 3
    And the following commands succeed:
      """
      [["TS.CREATE","east"],["TS.CREATE","west"],["TS.MADD","east",10,2,"east",20,4,"west",10,7,"west",30,9]]
      """
    When client "ts3" executes:
      """
      ["TS.NRANGE",2,"east","west",0,30]
      """
    Then the reply equals:
      """
      [[10,[{"$approx":2},{"$approx":7}]],[20,[{"$approx":4},{"type":"double","value":"nan"}]],[30,[{"type":"double","value":"nan"},{"$approx":9}]]]
      """
    When client "ts3" executes:
      """
      ["TS.NREVRANGE",2,"east","west",0,30]
      """
    Then the reply equals:
      """
      [[30,[{"type":"double","value":"nan"},{"$approx":9}]],[20,[{"$approx":4},{"type":"double","value":"nan"}]],[10,[{"$approx":2},{"$approx":7}]]]
      """
  @command_TS_READ
  Scenario: Blocking reads wait for the requested number of samples then return values in order
    Given a fresh ffkv server
    And client "ts3" uses RESP 3
    And the following commands succeed:
      """
      [["TS.CREATE","sensor"]]
      """
    When client "ts3" sends:
      """
      ["TS.READ","sensor",10,"BLOCK",0,2,"MAX_COUNT",2]
      """
    Then client "ts3" is blocked
    When I execute:
      """
      ["TS.ADD","sensor",10,2]
      """
    Then the reply equals:
      """
      10
      """
    When I execute:
      """
      ["TS.ADD","sensor",20,4]
      """
    Then the reply equals:
      """
      20
      """
    Then client "ts3" receives:
      """
      [[10,{"$approx":2}],[20,{"$approx":4}]]
      """
    When client "ts3" executes:
      """
      ["TS.READ","sensor",20,"MAX_COUNT",1]
      """
    Then the reply equals:
      """
      [[20,{"$approx":4}]]
      """

  Scenario: Time-weighted aggregation of a constant signal preserves its measured level
    Given a fresh ffkv server
    And client "ts3" uses RESP 3
    And the following commands succeed:
      """
      [["TS.CREATE","signal"],["TS.MADD","signal",0,4,"signal",50,4,"signal",100,4,"signal",150,4]]
      """
    When client "ts3" executes:
      """
      ["TS.RANGE","signal",0,149,"AGGREGATION","twa",100]
      """
    Then the reply equals:
      """
      [[0,{"$approx":4}],[100,{"$approx":4}]]
      """

  @command_TS_DEL @command_TS_CREATERULE @command_TS_GET @command_TS_RANGE
  Scenario: Deleting the latest source bucket durably reopens the previous compaction bucket
    Given a fresh ffkv server
    And client "ts3" uses RESP 3
    And the following commands succeed:
      """
      [["TS.CREATE","raw"],["TS.CREATE","avg"],["TS.CREATERULE","raw","avg","AGGREGATION","avg",10],["TS.ADD","raw",1,2],["TS.ADD","raw",10,8]]
      """
    When client "ts3" executes:
      """
      ["TS.DEL","raw",10,10]
      """
    Then the reply equals:
      """
      1
      """
    When client "ts3" executes:
      """
      ["TS.RANGE","avg","-","+"]
      """
    Then the reply equals:
      """
      []
      """
    When client "ts3" executes:
      """
      ["TS.GET","avg","LATEST"]
      """
    Then the reply equals:
      """
      [0,{"$approx":2}]
      """
    When ffkv restarts after a crash
    When client "ts3" executes:
      """
      ["TS.RANGE","avg","-","+"]
      """
    Then the reply equals:
      """
      []
      """
    When client "ts3" executes:
      """
      ["TS.RANGE","avg","-","+","LATEST"]
      """
    Then the reply equals:
      """
      [[0,{"$approx":2}]]
      """
    And the following commands succeed:
      """
      [["TS.ADD","raw",5,6],["TS.ADD","raw",10,8]]
      """
    When client "ts3" executes:
      """
      ["TS.RANGE","avg","-","+"]
      """
    Then the reply equals:
      """
      [[0,{"$approx":4}]]
      """

  @command_TS_DEL @command_TS_CREATERULE @command_TS_GET @command_TS_RANGE
  Scenario: Deleting every source sample resets compaction before earlier timestamps are reinserted
    Given a fresh ffkv server
    And client "ts3" uses RESP 3
    And the following commands succeed:
      """
      [["TS.CREATE","raw"],["TS.CREATE","avg"],["TS.CREATERULE","raw","avg","AGGREGATION","avg",10],["TS.ADD","raw",21,2],["TS.ADD","raw",30,8],["TS.DEL","raw",0,100]]
      """
    When client "ts3" executes:
      """
      ["TS.GET","avg","LATEST"]
      """
    Then the reply equals:
      """
      []
      """
    When ffkv restarts after a crash
    When client "ts3" executes:
      """
      ["TS.GET","avg","LATEST"]
      """
    Then the reply equals:
      """
      []
      """
    And the following commands succeed:
      """
      [["TS.ADD","raw",1,4]]
      """
    When client "ts3" executes:
      """
      ["TS.RANGE","avg","-","+"]
      """
    Then the reply equals:
      """
      []
      """
    When client "ts3" executes:
      """
      ["TS.GET","avg","LATEST"]
      """
    Then the reply equals:
      """
      [0,{"$approx":4}]
      """
    And the following commands succeed:
      """
      [["TS.ADD","raw",10,6]]
      """
    When client "ts3" executes:
      """
      ["TS.RANGE","avg","-","+"]
      """
    Then the reply equals:
      """
      [[0,{"$approx":4}]]
      """
