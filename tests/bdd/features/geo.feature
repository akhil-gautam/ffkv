# Contracts: https://redis.io/docs/latest/commands/geosearch/; https://redis.io/docs/latest/commands/geosearchstore/
Feature: Geospatial membership coordinates distances and stored searches

  @command_GEOADD @command_GEODIST @command_GEOHASH @command_GEOPOS
  Scenario: Coordinates and hashes identify indexed members and absent locations
    Given a fresh ffkv server
    When I execute:
      """
      ["GEOADD","sicily",13.361389,38.115556,"Palermo",15.087269,37.502669,"Catania"]
      """
    Then the reply equals:
      """
      2
      """
    When I execute:
      """
      ["GEOADD","sicily","NX",0,0,"Palermo"]
      """
    Then the reply equals:
      """
      0
      """
    When I execute:
      """
      ["GEODIST","sicily","Palermo","Catania","km"]
      """
    Then the reply equals:
      """
      {"$approx":{"value":166.2742,"abs":0.01}}
      """
    When I execute:
      """
      ["GEOHASH","sicily","Palermo","Catania","absent"]
      """
    Then the reply equals:
      """
      ["sqc8b49rny0","sqdtr74hyu0",null]
      """
    When I execute:
      """
      ["GEOPOS","sicily","Palermo","absent"]
      """
    Then the reply equals:
      """
      [[{"$approx":{"value":13.361389,"abs":1e-05}},{"$approx":{"value":38.115556,"abs":1e-05}}],null]
      """
    When I execute:
      """
      ["GEOADD","sicily",181,0,"invalid"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["GEOPOS","sicily","invalid"]
      """
    Then the reply equals:
      """
      [null]
      """
    When ffkv restarts after a crash
    When I execute:
      """
      ["GEOHASH","sicily","Palermo"]
      """
    Then the reply equals:
      """
      ["sqc8b49rny0"]
      """
  @command_GEORADIUS @command_GEORADIUSBYMEMBER @command_GEORADIUS_RO @command_GEORADIUSBYMEMBER_RO
  Scenario: Radius queries honor coordinate and member centers and distance ordering
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["GEOADD","sicily",13.361389,38.115556,"Palermo",15.087269,37.502669,"Catania"]]
      """
    When I execute:
      """
      ["GEORADIUS","sicily",13.361389,38.115556,200,"km","ASC"]
      """
    Then the reply equals:
      """
      ["Palermo","Catania"]
      """
    When I execute:
      """
      ["GEORADIUSBYMEMBER","sicily","Palermo",200,"km","ASC"]
      """
    Then the reply equals:
      """
      ["Palermo","Catania"]
      """
    When I execute:
      """
      ["GEORADIUS_RO","sicily",13.361389,38.115556,200,"km","ASC"]
      """
    Then the reply equals:
      """
      ["Palermo","Catania"]
      """
    When I execute:
      """
      ["GEORADIUSBYMEMBER_RO","sicily","Palermo",200,"km","ASC"]
      """
    Then the reply equals:
      """
      ["Palermo","Catania"]
      """
    When I execute:
      """
      ["GEORADIUSBYMEMBER","sicily","Palermo",10,"km","STORE","nearby"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["GEOHASH","nearby","Palermo","Catania"]
      """
    Then the reply equals:
      """
      ["sqc8b49rny0",null]
      """
  @command_GEOSEARCH @command_GEOSEARCHSTORE
  Scenario: Box search and stored distance scores replace a destination with selected members
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["GEOADD","sicily",13.361389,38.115556,"Palermo",15.087269,37.502669,"Catania"],["SET","nearby","old"]]
      """
    When I execute:
      """
      ["GEOSEARCH","sicily","FROMMEMBER","Palermo","BYBOX",400,400,"km","DESC","COUNT",1]
      """
    Then the reply equals:
      """
      ["Catania"]
      """
    When I execute:
      """
      ["GEOSEARCHSTORE","nearby","sicily","FROMMEMBER","Palermo","BYRADIUS",200,"km","STOREDIST"]
      """
    Then the reply equals:
      """
      2
      """
    When I execute:
      """
      ["ZSCORE","nearby","Palermo"]
      """
    Then the reply equals:
      """
      {"$approx":0}
      """
    When I execute:
      """
      ["ZSCORE","nearby","Catania"]
      """
    Then the reply equals:
      """
      {"$approx":{"value":166.2742,"abs":0.01}}
      """

  @command_GEOADD @command_GEOSEARCHSTORE
  Scenario Outline: Underflowing geo arguments cannot delete an aliased source destination
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["GEOADD","geo","1","1","a"],["PEXPIRE","geo","60000"]]
      """
    When I execute:
      """
      ["GEOSEARCHSTORE","geo","geo","FROMLONLAT","<longitude>","<latitude>","BYRADIUS","<radius>","m"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["GEOSEARCH","geo","FROMMEMBER","a","BYRADIUS","1","m"]
      """
    Then the reply equals:
      """
      ["a"]
      """
    When I execute:
      """
      ["ZCARD","geo"]
      """
    Then the reply equals:
      """
      1
      """

    Examples:
      | longitude | latitude | radius  |
      | 0         | 0        | 1e-9999 |
      | 1e-9999   | 0        | 1       |
      | 0         | 1e-9999  | 1       |

  @command_GEOADD @command_GEOSEARCHSTORE
  Scenario Outline: Explicit zero and representable subnormal geo numbers are valid
    Given a fresh ffkv server
    When I execute:
      """
      ["GEOADD","geo","<number>","<number>","a"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["GEOSEARCHSTORE","geo","geo","FROMMEMBER","a","BYRADIUS","<number>","m"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["ZCARD","geo"]
      """
    Then the reply equals:
      """
      1
      """

    Examples:
      | number                  |
      | 0                       |
      | -0                      |
      | 0e-9999                 |
      | 4.9406564584124654e-324 |
      | 1e-320                  |
