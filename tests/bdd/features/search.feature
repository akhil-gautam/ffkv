Feature: Redis 8.10 search indexes and query pipelines
  Search metadata is independent of document keys, and source writes maintain real indexes.

  @command_FT_CREATE @command_FT_SEARCH
  Scenario: Numeric signed zero bounds share equality without invalid ranges
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["HSET","doc:zero","n","-0"],["FT.CREATE","idx","ON","HASH","PREFIX","1","doc:","SCHEMA","n","NUMERIC"]]
      """
    When I execute:
      """
      ["FT.SEARCH","idx","@n:[0 -0]","NOCONTENT","DIALECT","2"]
      """
    Then the reply equals:
      """
      [1,"doc:zero"]
      """
    When I execute:
      """
      ["FT.SEARCH","idx","@n:[0 0]","NOCONTENT","DIALECT","2"]
      """
    Then the reply equals:
      """
      [1,"doc:zero"]
      """
    When I execute:
      """
      ["FT.SEARCH","idx","@n:[(0 -0]","NOCONTENT","DIALECT","2"]
      """
    Then the reply equals:
      """
      [0]
      """
    Given the following commands succeed:
      """
      [["HSET","doc:zero","n","0"]]
      """
    When I execute:
      """
      ["FT.SEARCH","idx","@n:[-0 -0]","NOCONTENT","DIALECT","2"]
      """
    Then the reply equals:
      """
      [1,"doc:zero"]
      """
    Given the following commands succeed:
      """
      [["DEL","doc:zero"]]
      """
    When I execute:
      """
      ["FT.SEARCH","idx","@n:[0 -0]","NOCONTENT","DIALECT","2"]
      """
    Then the reply equals:
      """
      [0]
      """

  @command_FT_CREATE @command_FT_SEARCH
  Scenario: Empty tag queries follow initial scans updates deletion and rebuild
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["HSET","doc:empty","tag",""],["FT.CREATE","idx","ON","HASH","PREFIX","1","doc:","SCHEMA","tag","TAG","INDEXEMPTY"]]
      """
    When I execute:
      """
      ["FT.SEARCH","idx","@tag:{\"\"}","NOCONTENT","DIALECT","2"]
      """
    Then the reply equals:
      """
      [1,"doc:empty"]
      """
    Given the following commands succeed:
      """
      [["HSET","doc:empty","tag","red"]]
      """
    When I execute:
      """
      ["FT.SEARCH","idx","@tag:{\"\"}","NOCONTENT","DIALECT","2"]
      """
    Then the reply equals:
      """
      [0]
      """
    Given the following commands succeed:
      """
      [["HSET","doc:empty","tag",""],["FT.DROPINDEX","idx"],["FT.CREATE","idx","PREFIX","1","doc:","SCHEMA","tag","TAG","INDEXEMPTY"]]
      """
    When I execute:
      """
      ["FT.SEARCH","idx","@tag:{\"\"}","NOCONTENT","DIALECT","2"]
      """
    Then the reply equals:
      """
      [1,"doc:empty"]
      """
    Given the following commands succeed:
      """
      [["DEL","doc:empty"]]
      """
    When I execute:
      """
      ["FT.SEARCH","idx","@tag:{\"\"}","NOCONTENT","DIALECT","2"]
      """
    Then the reply equals:
      """
      [0]
      """

  @command_FT_CREATE @command_FT_SEARCH
  Scenario: Initial scans and typed intersections find only eligible documents
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["HSET","doc:1","title","red shoes","price","10","tags","sale,red"],["HSET","doc:2","title","blue shoes","price","20","tags","sale,blue"],["HSET","other:1","title","blue shoes","price","30","tags","sale"],["FT.CREATE","idx","ON","HASH","PREFIX","1","doc:","SCHEMA","title","TEXT","price","NUMERIC","SORTABLE","tags","TAG"]]
      """
    When I execute:
      """
      ["FT.SEARCH","idx","@title:shoes @price:[(10 +inf] @tags:{sale}","NOCONTENT","DIALECT","2"]
      """
    Then the reply equals:
      """
      [1,"doc:2"]
      """
    When I execute:
      """
      ["FT.SEARCH","idx","shoes","NOCONTENT","SORTBY","price","ASC","LIMIT","1","1"]
      """
    Then the reply equals:
      """
      [2,"doc:2"]
      """

  @command_FT_SEARCH
  Scenario: Hash mutations remove old postings without losing the source value
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["FT.CREATE","idx","PREFIX","1","doc:","SCHEMA","title","TEXT","price","NUMERIC"],["HSET","doc:1","title","old shoes","price","10"],["HSET","doc:1","title","new boots"],["HINCRBY","doc:1","price","5"]]
      """
    When I execute:
      """
      ["FT.SEARCH","idx","shoes","NOCONTENT"]
      """
    Then the reply equals:
      """
      [0]
      """
    When I execute:
      """
      ["FT.SEARCH","idx","boots @price:[15 15]","RETURN","3","title","AS","name","DIALECT","2"]
      """
    Then the reply equals:
      """
      [1,"doc:1",["name","new boots"]]
      """

  @command_FT_INFO @command_FT_SEARCH
  Scenario: Invalid indexed values reject the document but never roll back HSET
    Given a fresh ffkv server
    Given client "info" uses RESP 3
    And the following commands succeed:
      """
      [["FT.CREATE","idx","PREFIX","1","doc:","SCHEMA","title","TEXT","price","NUMERIC"],["HSET","doc:1","title","boots","price","10"],["HSET","doc:1","price","not-a-number"]]
      """
    When I execute:
      """
      ["HGET","doc:1","price"]
      """
    Then the reply equals:
      """
      "not-a-number"
      """
    When I execute:
      """
      ["FT.SEARCH","idx","boots","NOCONTENT"]
      """
    Then the reply equals:
      """
      [0]
      """
    When client "info" executes:
      """
      ["FT.INFO","idx"]
      """
    Then the reply contains:
      """
      {"index_name":"idx","num_docs":0,"hash_indexing_failures":1}
      """
    Given the following commands succeed:
      """
      [["HSET","doc:1","price","12"]]
      """
    When I execute:
      """
      ["FT.SEARCH","idx","boots @price:[12 12]","NOCONTENT","DIALECT","2"]
      """
    Then the reply equals:
      """
      [1,"doc:1"]
      """

  @command_FT_ALIASADD @command_FT_ALIASDEL @command_FT_ALIASUPDATE @command_FT_ALIASLIST
  Scenario: Alias creation rebinding and deletion affect metadata only
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["FT.CREATE","first","PREFIX","1","a:","SCHEMA","title","TEXT"],["FT.CREATE","second","PREFIX","1","b:","SCHEMA","title","TEXT"],["HSET","a:1","title","shoes"],["HSET","b:1","title","boots"],["FT.ALIASADD","current","first"],["FT.ALIASADD","other","first"]]
      """
    When I execute:
      """
      ["FT.SEARCH","current","shoes","NOCONTENT"]
      """
    Then the reply equals:
      """
      [1,"a:1"]
      """
    When I execute:
      """
      ["FT.ALIASLIST","first"]
      """
    Then the reply contains:
      """
      ["current","other"]
      """
    Given the following commands succeed:
      """
      [["FT.ALIASUPDATE","current","second"],["FT.ALIASDEL","other"]]
      """
    When I execute:
      """
      ["FT.SEARCH","current","boots","NOCONTENT"]
      """
    Then the reply equals:
      """
      [1,"b:1"]
      """
    When I execute:
      """
      ["FT.ALIASLIST","first"]
      """
    Then the reply equals:
      """
      []
      """
    When I execute:
      """
      ["FT.ALIASLIST","current"]
      """
    Then the reply is an error with code "ERR"

  @command_FT_ALTER
  Scenario: ALTER validates a full new schema before rescanning existing documents
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["FT.CREATE","idx","PREFIX","1","doc:","SCHEMA","title","TEXT"],["HSET","doc:1","title","boots","price","19"],["FT.ALTER","idx","SCHEMA","ADD","price","NUMERIC","SORTABLE"]]
      """
    When I execute:
      """
      ["FT.SEARCH","idx","@price:[19 19]","NOCONTENT","DIALECT","2"]
      """
    Then the reply equals:
      """
      [1,"doc:1"]
      """
    When I execute:
      """
      ["FT.ALTER","idx","SCHEMA","ADD","price","TEXT"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["FT.SEARCH","idx","@price:[19 19]","NOCONTENT","DIALECT","2"]
      """
    Then the reply equals:
      """
      [1,"doc:1"]
      """

  @command_FT_CONFIG_GET @command_FT_CONFIG_SET
  Scenario: Runtime configuration round trips and invalid changes are atomic
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["FT.CONFIG","SET","TIMEOUT","1000"],["FT.CONFIG","SET","DEFAULT_DIALECT","2"]]
      """
    When I execute:
      """
      ["FT.CONFIG","GET","TIMEOUT"]
      """
    Then the reply equals:
      """
      [["TIMEOUT","1000"]]
      """
    When I execute:
      """
      ["FT.CONFIG","SET","DEFAULT_DIALECT","9"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["FT.CONFIG","GET","DEFAULT_DIALECT"]
      """
    Then the reply equals:
      """
      [["DEFAULT_DIALECT","2"]]
      """
    When I execute:
      """
      ["FT.CONFIG","GET","DOES_NOT_EXIST"]
      """
    Then the reply equals:
      """
      []
      """

  @command_FT_DICTADD @command_FT_DICTDEL @command_FT_DICTDUMP @command_FT_SPELLCHECK
  Scenario: Dictionaries supplement real document-frequency spelling suggestions
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["FT.CREATE","idx","PREFIX","1","doc:","SCHEMA","title","TEXT"],["HSET","doc:1","title","hello"],["FT.DICTADD","words","help","helmet"]]
      """
    When I execute:
      """
      ["FT.DICTADD","words","help"]
      """
    Then the reply equals:
      """
      0
      """
    When I execute:
      """
      ["FT.SPELLCHECK","idx","helo","TERMS","INCLUDE","words"]
      """
    Then the reply contains:
      """
      [["TERM","helo",[["1","hello"],["0","help"]]]]
      """
    When I execute:
      """
      ["FT.DICTDEL","words","helmet","absent"]
      """
    Then the reply equals:
      """
      1
      """
    When I execute:
      """
      ["FT.DICTDUMP","words"]
      """
    Then the reply equals:
      """
      ["help"]
      """

  @command_FT_SYNUPDATE @command_FT_SYNDUMP
  Scenario: Synonym updates preserve skipped document coverage through recovery
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["FT.CREATE","idx","PREFIX","1","doc:","SCHEMA","title","TEXT"],["HSET","doc:1","title","boy"],["FT.SYNUPDATE","idx","family","SKIPINITIALSCAN","boy","child"]]
      """
    When I execute:
      """
      ["FT.SEARCH","idx","child","NOCONTENT"]
      """
    Then the reply equals:
      """
      [0]
      """
    When ffkv restarts after a crash
    When I execute:
      """
      ["FT.SEARCH","idx","child","NOCONTENT"]
      """
    Then the reply equals:
      """
      [0]
      """
    Given the following commands succeed:
      """
      [["HSET","doc:1","unindexed","changed"]]
      """
    When I execute:
      """
      ["FT.SEARCH","idx","child","NOCONTENT"]
      """
    Then the reply equals:
      """
      [1,"doc:1"]
      """
    When I execute:
      """
      ["FT.SYNDUMP","idx"]
      """
    Then the reply equals:
      """
      ["boy",["family"],"child",["family"]]
      """

  @command_FT_TAGVALS
  Scenario: Tags preserve interior spaces and normalize only configured case
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["FT.CREATE","idx","PREFIX","1","doc:","SCHEMA","tags","TAG","exact","TAG","CASESENSITIVE"],["HSET","doc:1","tags","  Big Sale  ,BLUE","exact","Sale,sale"]]
      """
    When I execute:
      """
      ["FT.TAGVALS","idx","tags"]
      """
    Then the reply contains:
      """
      ["big sale","blue"]
      """
    When I execute:
      """
      ["FT.TAGVALS","idx","exact"]
      """
    Then the reply contains:
      """
      ["Sale","sale"]
      """
    When I execute:
      """
      ["FT.SEARCH","idx","@tags:{big sale}","NOCONTENT","DIALECT","2"]
      """
    Then the reply equals:
      """
      [1,"doc:1"]
      """

  @command_FT__LIST
  Scenario: Index aliases and dictionaries never become Redis keys or real indexes
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["FT.CREATE","idx","SCHEMA","title","TEXT"],["FT.ALIASADD","view","idx"],["FT.DICTADD","words","hello"]]
      """
    When I execute:
      """
      ["FT._LIST"]
      """
    Then the reply equals:
      """
      ["idx"]
      """
    When I execute:
      """
      ["KEYS","*"]
      """
    Then the reply equals:
      """
      []
      """

  @command_FT_DROPINDEX
  Scenario: DROPINDEX keeps documents unless DD explicitly deletes indexed sources
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["FT.CREATE","keep","PREFIX","1","doc:","SCHEMA","title","TEXT"],["HSET","doc:1","title","boots"],["FT.DROPINDEX","keep"]]
      """
    When I execute:
      """
      ["HGET","doc:1","title"]
      """
    Then the reply equals:
      """
      "boots"
      """
    Given the following commands succeed:
      """
      [["FT.CREATE","remove","PREFIX","1","doc:","SCHEMA","title","TEXT"],["HSET","other:1","title","safe"],["FT.DROPINDEX","remove","DD"]]
      """
    When I execute:
      """
      ["EXISTS","doc:1","other:1"]
      """
    Then the reply equals:
      """
      1
      """

  @command_FT_EXPLAIN @command_FT_EXPLAINCLI
  Scenario: Explain APIs expose the parsed indexed execution tree
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["FT.CREATE","idx","SCHEMA","title","TEXT","price","NUMERIC"]]
      """
    When I execute:
      """
      ["FT.EXPLAIN","idx","@price:[10 20]","DIALECT","2"]
      """
    Then the reply contains text "NUMERIC"
    When I execute:
      """
      ["FT.EXPLAINCLI","idx","hello|world","DIALECT","2"]
      """
    Then the reply contains:
      """
      ["UNION {"]
      """

  @command_FT_AGGREGATE
  Scenario: Ordered transformations feed genuine grouped reducers
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["FT.CREATE","idx","PREFIX","1","doc:","SCHEMA","dept","TAG","price","NUMERIC","SORTABLE"],["HSET","doc:1","dept","a","price","10"],["HSET","doc:2","dept","a","price","20"],["HSET","doc:3","dept","b","price","30"]]
      """
    When I execute:
      """
      ["FT.AGGREGATE","idx","*","LOAD","2","@dept","@price","APPLY","@price * 2","AS","doubled","FILTER","@doubled >= 20","GROUPBY","1","@dept","REDUCE","COUNT","0","AS","n","REDUCE","SUM","1","@doubled","AS","total","SORTBY","2","@dept","ASC"]
      """
    Then the reply equals:
      """
      [2,["dept","a","n","2","total","60"],["dept","b","n","1","total","60"]]
      """
    When I execute:
      """
      ["FT.AGGREGATE","idx","*","APPLY","unknown_function(@price)","AS","bad"]
      """
    Then the reply is an error with code "ERR"

  @command_FT_AGGREGATE
  Scenario: Global reducers consume all input rows exactly once
    Given a fresh ffkv server
    Given client "aggregate" uses RESP 3
    And the following commands succeed:
      """
      [["FT.CREATE","idx","PREFIX","1","doc:","SCHEMA","g","TAG","SORTABLE","n","NUMERIC","SORTABLE"],["HSET","doc:1","g","a","n","1"],["HSET","doc:2","g","a","n","3"],["HSET","doc:3","g","b","n","8"]]
      """
    When client "aggregate" executes:
      """
      ["FT.AGGREGATE","idx","*","GROUPBY","0","REDUCE","COUNT","0","AS","count","REDUCE","COUNT_DISTINCT","1","@g","AS","distinct","REDUCE","COUNT_DISTINCTISH","1","@g","AS","estimate","REDUCE","STDDEV","1","@n","AS","stddev","REDUCE","QUANTILE","2","@n","0.5","AS","median"]
      """
    Then the reply contains:
      """
      {"total_results":1,"results":[{"extra_attributes":{"count":{"$approx":3},"distinct":{"$approx":2},"estimate":{"$approx":2},"stddev":{"$approx":{"value":3.605551275463989,"abs":1e-12}},"median":{"$approx":3}}}]}
      """
    When client "aggregate" executes:
      """
      ["FT.AGGREGATE","idx","*","GROUPBY","0","REDUCE","RANDOM_SAMPLE","2","@n","2","AS","sample","GROUPBY","1","@sample","FILTER","@sample == 1 || @sample == 3 || @sample == 8","GROUPBY","0","REDUCE","COUNT","0","AS","sample_size"]
      """
    Then the reply contains:
      """
      {"total_results":1,"results":[{"extra_attributes":{"sample_size":{"$approx":2}}}]}
      """
    When client "aggregate" executes:
      """
      ["FT.AGGREGATE","idx","@n:[99 99]","GROUPBY","0","REDUCE","COUNT","0","AS","count","REDUCE","SUM","1","@n","AS","sum","REDUCE","COLLECT","3","FIELDS","1","@g","AS","records"]
      """
    Then the reply contains:
      """
      {"total_results":1,"results":[{"extra_attributes":{"count":{"$approx":0},"sum":{"$approx":0},"records":[]}}]}
      """

  @command_FT_AGGREGATE
  Scenario: Repeated grouping preserves stage order and sorted distinct records
    Given a fresh ffkv server
    Given client "aggregate" uses RESP 3
    And the following commands succeed:
      """
      [["FT.CREATE","idx","PREFIX","1","doc:","SCHEMA","g","TAG","SORTABLE","n","NUMERIC","SORTABLE"],["HSET","doc:1","g","a","n","1"],["HSET","doc:2","g","a","n","3"],["HSET","doc:3","g","b","n","8"]]
      """
    When client "aggregate" executes:
      """
      ["FT.AGGREGATE","idx","*","LIMIT","0","1","GROUPBY","1","@g","REDUCE","SUM","1","@n","AS","sum"]
      """
    Then the reply contains:
      """
      {"total_results":1,"results":[{"extra_attributes":{"g":"a","sum":{"$approx":1}}}]}
      """
    When client "aggregate" executes:
      """
      ["FT.AGGREGATE","idx","*","GROUPBY","1","@g","REDUCE","SUM","1","@n","AS","sum","SORTBY","2","@sum","DESC","LIMIT","0","1","APPLY","@sum * 2","AS","double","GROUPBY","0","REDUCE","SUM","1","@double","AS","total"]
      """
    Then the reply contains:
      """
      {"total_results":1,"results":[{"extra_attributes":{"total":{"$approx":16}}}]}
      """
    When I execute:
      """
      ["FT.AGGREGATE","idx","*","GROUPBY","0","REDUCE","COLLECT","11","FIELDS","1","@g","DISTINCT","SORTBY","2","@n","DESC","LIMIT","0","2","AS","records"]
      """
    Then the reply equals:
      """
      [1,["records",[["g","b"],["g","a"]]]]
      """

  @command_FT_CURSOR_READ
  Scenario: A cursor advances real pipeline state without duplicate or skipped rows
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["FT.CREATE","idx","PREFIX","1","doc:","SCHEMA","title","TEXT","SORTABLE"],["HSET","doc:1","title","a"],["HSET","doc:2","title","b"],["HSET","doc:3","title","c"],["HSET","doc:4","title","d"]]
      """
    When I execute:
      """
      ["FT.AGGREGATE","idx","*","WITHCOUNT","LOAD","1","@title","SORTBY","2","@title","ASC","WITHCURSOR","COUNT","1"]
      """
    Then the reply has JSON path "0" equal to:
      """
      [4,["title","a"]]
      """
    When I save the reply at JSON pointer "/1" as "cursor"
    When I execute:
      """
      ["FT.CURSOR","READ","idx",{"saved":"cursor"},"COUNT","2"]
      """
    Then the reply has JSON path "0" equal to:
      """
      [4,["title","b"],["title","c"]]
      """
    When I save the reply at JSON pointer "/1" as "cursor"
    When I execute:
      """
      ["FT.CURSOR","READ","idx",{"saved":"cursor"},"COUNT","10"]
      """
    Then the reply equals:
      """
      [[4,["title","d"]],0]
      """
    When I execute:
      """
      ["FT.CURSOR","READ","idx",{"saved":"cursor"}]
      """
    Then the reply is an error with code "ERR"

  @command_FT_CURSOR_DEL
  Scenario: Deleted cursor handles cannot be reused
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["FT.CREATE","idx","PREFIX","1","doc:","SCHEMA","title","TEXT"],["HSET","doc:1","title","first"],["HSET","doc:2","title","second"]]
      """
    When I execute:
      """
      ["FT.AGGREGATE","idx","*","LOAD","1","@title","WITHCURSOR","COUNT","1"]
      """
    When I save the reply at JSON pointer "/1" as "cursor"
    When I execute:
      """
      ["FT.CURSOR","DEL","idx",{"saved":"cursor"}]
      """
    Then the reply equals:
      """
      "OK"
      """
    When I execute:
      """
      ["FT.CURSOR","READ","idx",{"saved":"cursor"}]
      """
    Then the reply is an error with code "ERR"

  @command_FT_PROFILE
  Scenario: Profiling returns the actual query result and measured execution metadata
    Given a fresh ffkv server
    Given client "profile" uses RESP 3
    And the following commands succeed:
      """
      [["FT.CREATE","idx","PREFIX","1","doc:","SCHEMA","title","TEXT"],["HSET","doc:1","title","boots"],["HSET","doc:2","title","sandals"]]
      """
    When client "profile" executes:
      """
      ["FT.PROFILE","idx","SEARCH","LIMITED","QUERY","boots","NOCONTENT"]
      """
    Then the reply contains:
      """
      {"Results":{"total_results":1,"results":[{"id":"doc:1"}]},"Profile":{"Shards":[{"Result processors profile":[{"Results processed":1}]}]}}
      """
    And the reply has JSON path "Profile.Shards.0.Total profile time" with a nonnegative number
    And the reply has JSON path "Profile.Shards.0.Parsing time" with a nonnegative number
    And the reply has JSON path "Profile.Shards.0.Iterators profile.Time" with a nonnegative number
    And the reply has JSON path "Profile.Shards.0.Iterators profile.Number of reading operations" with an integer greater than 0
    When client "profile" executes:
      """
      ["FT.PROFILE","idx","SEARCH","QUERY","boots|sandals","NOCONTENT"]
      """
    Then the reply contains:
      """
      {"Results":{"total_results":2,"results":[{"id":"doc:1"},{"id":"doc:2"}]},"Profile":{"Shards":[{"Result processors profile":[{"Results processed":2}]}]}}
      """
    And the reply has JSON path "Profile.Shards.0.Total profile time" with a nonnegative number

  @command_FT_SEARCH
  Scenario Outline: Each vector algorithm maintains a real index across updates and deletions
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["FT.CREATE","idx","PREFIX","1","doc:","SCHEMA","title","TEXT","v","VECTOR","<algorithm>","6","TYPE","FLOAT32","DIM","2","DISTANCE_METRIC","L2"],["HSET","doc:1","title","near","v",{"base64":"AAAAAAAAAAA="}],["HSET","doc:2","title","far","v",{"base64":"AAAAQAAAAAA="}]]
      """
    When I execute:
      """
      ["FT.SEARCH","idx","*=>[KNN 2 @v $q AS distance]","PARAMS","2","q",{"base64":"AAAAAAAAAAA="},"SORTBY","distance","ASC","RETURN","1","distance","DIALECT","2"]
      """
    Then the reply equals:
      """
      [2,"doc:1",["distance","0"],"doc:2",["distance","4"]]
      """
    Given the following commands succeed:
      """
      [["HSET","doc:2","v",{"base64":"AACAPwAAAAA="}],["DEL","doc:1"]]
      """
    When I execute:
      """
      ["FT.SEARCH","idx","*=>[KNN 2 @v $q AS distance]","PARAMS","2","q",{"base64":"AAAAAAAAAAA="},"SORTBY","distance","RETURN","1","distance","DIALECT","2"]
      """
    Then the reply equals:
      """
      [1,"doc:2",["distance","1"]]
      """
    Examples:
      | algorithm  |
      | FLAT       |
      | HNSW       |
      | SVS-VAMANA |

  @command_FT_HYBRID
  Scenario: Reciprocal-rank fusion combines the union of text and vector branches
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["FT.CREATE","idx","PREFIX","1","doc:","SCORE_FIELD","rank","SCHEMA","title","TEXT","v","VECTOR","FLAT","6","TYPE","FLOAT32","DIM","2","DISTANCE_METRIC","L2"],["HSET","doc:both","title","rare","rank","1","v",{"base64":"AAAAAAAAAAA="}],["HSET","doc:text","title","rare","rank","0.5","v",{"base64":"AAAgQQAAAAA="}],["HSET","doc:vector","title","blue","rank","1","v",{"base64":"AACAPwAAAAA="}]]
      """
    When I execute:
      """
      ["FT.HYBRID","idx","SEARCH","rare","SCORER","DOCSCORE","VSIM","@v","$q","KNN","2","K","2","COMBINE","RRF","2","CONSTANT","60","PARAMS","2","q",{"base64":"AAAAAAAAAAA="},"LIMIT","0","10"]
      """
    Then the reply contains:
      """
      ["total_results",3,[["__key","doc:both","__score",{"$approx":{"value":0.03278688524590164,"abs":1e-12}}],["__key","doc:text"],["__key","doc:vector"]]]
      """
    When I execute:
      """
      ["FT.HYBRID","idx","SEARCH","rare","SCORER","DOCSCORE","VSIM","@v","$q","KNN","2","K","2","COMBINE","RRF","2","CONSTANT","60","PARAMS","2","q",{"base64":"AAAAAAAAAAA="},"GROUPBY","0","REDUCE","COUNT","0","AS","count"]
      """
    Then the reply contains:
      """
      ["total_results",1,[["count",{"$approx":3}]]]
      """
    When I execute:
      """
      ["FT.HYBRID","idx","SEARCH","rare","VSIM","@v","$q","PARAMS","2","q",{"base64":"AAAAAAAAAAA="},"DIALECT","2"]
      """
    Then the reply is an error with code "ERR"

  @command_FT_SEARCH
  Scenario: Geographic radius and nonrectangular shape predicates use indexed geometry
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["FT.CREATE","idx","PREFIX","1","doc:","SCHEMA","loc","GEO","shape","GEOSHAPE","FLAT"],["HSET","doc:1","loc","0,0","shape","POLYGON((0 0,4 0,0 4,0 0))"],["HSET","doc:2","loc","30,30","shape","POLYGON((3 3,4 3,3 4,3 3))"]]
      """
    When I execute:
      """
      ["FT.SEARCH","idx","@loc:[0 0 1 km]","NOCONTENT","DIALECT","2"]
      """
    Then the reply equals:
      """
      [1,"doc:1"]
      """
    When I execute:
      """
      ["FT.SEARCH","idx","@shape:[CONTAINS $point]","PARAMS","2","point","POINT(3.2 3.2)","NOCONTENT","DIALECT","3"]
      """
    Then the reply equals:
      """
      [1,"doc:2"]
      """

  @command_FT_CREATE @command_FT_SEARCH
  Scenario: JSON multivalue indexing follows all matching paths and subsequent JSON writes
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["JSON.SET","doc:1","$","{\"titles\":[\"red shoes\",\"blue boots\"],\"prices\":[10,20],\"tags\":[\"sale\",\"new\"]}"],["FT.CREATE","idx","ON","JSON","PREFIX","1","doc:","SCHEMA","$.titles[*]","AS","title","TEXT","$.prices[*]","AS","price","NUMERIC","$.tags[*]","AS","tag","TAG"]]
      """
    When I execute:
      """
      ["FT.SEARCH","idx","boots @price:[20 20] @tag:{sale}","NOCONTENT","DIALECT","3"]
      """
    Then the reply equals:
      """
      [1,"doc:1"]
      """
    Given the following commands succeed:
      """
      [["JSON.SET","doc:1","$.titles[1]","\"green sandals\""],["JSON.NUMINCRBY","doc:1","$.prices[1]",5]]
      """
    When ffkv restarts after a crash
    When I execute:
      """
      ["FT.SEARCH","idx","boots","NOCONTENT","DIALECT","3"]
      """
    Then the reply equals:
      """
      [0]
      """
    When I execute:
      """
      ["FT.SEARCH","idx","sandals @price:[25 25]","NOCONTENT","DIALECT","3"]
      """
    Then the reply equals:
      """
      [1,"doc:1"]
      """

  @command_FT_SEARCH
  Scenario: Phrase fuzzy stemming and highlighting share genuine match positions
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["FT.CREATE","idx","PREFIX","1","doc:","STOPWORDS","0","SCHEMA","title","TEXT"],["HSET","doc:1","title","running red shoes"],["HSET","doc:2","title","red running shoes"]]
      """
    When I execute:
      """
      ["FT.SEARCH","idx","\"red shoes\"","NOCONTENT","DIALECT","2"]
      """
    Then the reply equals:
      """
      [1,"doc:1"]
      """
    When I execute:
      """
      ["FT.SEARCH","idx","run %shoos%","NOCONTENT","LIMIT","0","0","DIALECT","2"]
      """
    Then the reply equals:
      """
      [2]
      """
    When I execute:
      """
      ["FT.SEARCH","idx","@title:\"red shoes\"","RETURN","1","title","HIGHLIGHT","FIELDS","1","title","TAGS","<em>","</em>","DIALECT","2"]
      """
    Then the reply equals:
      """
      [1,"doc:1",["title","running <em>red</em> <em>shoes</em>"]]
      """
    When I execute:
      """
      ["FT.SEARCH","idx","run","VERBATIM","NOCONTENT","DIALECT","2"]
      """
    Then the reply equals:
      """
      [0]
      """

  @command_FT_SEARCH @command_FT_CONFIG_SET
  Scenario: Minimum stem length governs indexed words without blocking short query roots
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["FT.CONFIG","SET","MINSTEMLEN","4"],["FT.CREATE","idx","PREFIX","1","doc:","STOPWORDS","0","SCHEMA","title","TEXT"],["HSET","doc:1","title","fry"],["HSET","doc:2","title","fries"]]
      """
    When I execute:
      """
      ["FT.SEARCH","idx","fried","NOCONTENT","DIALECT","2"]
      """
    Then the reply equals:
      """
      [1,"doc:2"]
      """
    When I execute:
      """
      ["FT.SEARCH","idx","fry","NOCONTENT","LIMIT","0","0","DIALECT","2"]
      """
    Then the reply equals:
      """
      [2]
      """

  @command_FT_SEARCH
  Scenario: Missing and empty fields remain distinct searchable states
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["FT.CREATE","idx","PREFIX","1","doc:","SCHEMA","title","TEXT","INDEXEMPTY","INDEXMISSING","tag","TAG","INDEXEMPTY","INDEXMISSING"],["HSET","doc:empty","title","","tag",""],["HSET","doc:missing","other","value"]]
      """
    When I execute:
      """
      ["FT.SEARCH","idx","@title:\"\"","NOCONTENT","DIALECT","2"]
      """
    Then the reply equals:
      """
      [1,"doc:empty"]
      """
    When I execute:
      """
      ["FT.SEARCH","idx","ismissing(@tag)","NOCONTENT","DIALECT","2"]
      """
    Then the reply equals:
      """
      [1,"doc:missing"]
      """

  @command_FT_SEARCH
  Scenario: Rename expiration deletion and type replacement do not leave stale postings
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["FT.CREATE","idx","PREFIX","1","doc:","SCHEMA","title","TEXT","tag","TAG","INDEXMISSING"],["HSET","doc:1","title","boots","tag","sale"],["RENAME","doc:1","doc:2"],["HEXPIREAT","doc:2",1,"FIELDS",1,"tag"]]
      """
    When I execute:
      """
      ["FT.SEARCH","idx","boots ismissing(@tag)","NOCONTENT","DIALECT","2"]
      """
    Then the reply equals:
      """
      [1,"doc:2"]
      """
    Given the following commands succeed:
      """
      [["SET","doc:2","replacement"]]
      """
    When I execute:
      """
      ["FT.SEARCH","idx","boots","NOCONTENT"]
      """
    Then the reply equals:
      """
      [0]
      """
    Given the following commands succeed:
      """
      [["HSET","doc:3","title","boots"],["PEXPIREAT","doc:3",1]]
      """
    When I execute:
      """
      ["FT.SEARCH","idx","boots","NOCONTENT"]
      """
    Then the reply equals:
      """
      [0]
      """

  @command_FT_CREATE @command_FT_ALTER @command_FT_ALIASADD @command_FT_DICTDUMP
  Scenario: Recovery retains intentionally unindexed initial data and metadata namespaces
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["HSET","doc:old","title","old","price","10"],["FT.CREATE","idx","PREFIX","1","doc:","SKIPINITIALSCAN","SCHEMA","title","TEXT"],["FT.ALTER","idx","SKIPINITIALSCAN","SCHEMA","ADD","price","NUMERIC"],["HSET","doc:new","title","new","price","20"],["FT.ALIASADD","view","idx"],["FT.DICTADD","words","durable"]]
      """
    When ffkv restarts after a crash
    When I execute:
      """
      ["FT.SEARCH","view","*","NOCONTENT"]
      """
    Then the reply equals:
      """
      [1,"doc:new"]
      """
    When I execute:
      """
      ["FT.SEARCH","idx","@price:[10 10]","NOCONTENT","DIALECT","2"]
      """
    Then the reply equals:
      """
      [0]
      """
    When I execute:
      """
      ["FT.DICTDUMP","words"]
      """
    Then the reply equals:
      """
      ["durable"]
      """
    When I execute:
      """
      ["TYPE","idx"]
      """
    Then the reply equals:
      """
      "none"
      """

  @command_FT_SEARCH @command_FT_INFO
  Scenario: ACLs authorize prefix namespaces rather than the index name
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["FT.CREATE","restricted","PREFIX","1","doc:","SCHEMA","title","TEXT"],["FT.CREATE","global","SCHEMA","title","TEXT"],["HSET","doc:1","title","boots"],["ACL","SETUSER","reader","on",">search-password","+@all","~doc:*"]]
      """
    When client "reader" executes:
      """
      ["AUTH","reader","search-password"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "reader" executes:
      """
      ["FT.SEARCH","restricted","boots","NOCONTENT"]
      """
    Then the reply equals:
      """
      [1,"doc:1"]
      """
    When client "reader" executes:
      """
      ["FT.INFO","global"]
      """
    Then the reply is an error with code "NOPERM"

  @command_FT_CREATE
  Scenario: Search metadata cannot be created outside database zero
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["SELECT",1]]
      """
    When I execute:
      """
      ["FT.CREATE","wrong-db","SCHEMA","title","TEXT"]
      """
    Then the reply is an error with code "ERR"

  @command_FT_SEARCH @command_FT_AGGREGATE @command_FT_HYBRID @command_FT_CREATE
  Scenario Outline: Invalid query schema and pipeline options fail without document mutation
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["FT.CREATE","idx","PREFIX","1","doc:","SCHEMA","title","TEXT","price","NUMERIC"],["HSET","doc:1","title","safe","price","10"]]
      """
    When I execute:
      """
      <command>
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["HGET","doc:1","title"]
      """
    Then the reply equals:
      """
      "safe"
      """
    Examples:
      | command                                                                                          |
      | ["FT.SEARCH","idx","("]                                                                          |
      | ["FT.SEARCH","idx","safe","EXPLAINSCORE"]                                                          |
      | ["FT.SEARCH","idx","safe","PARAMS",1,"missing","DIALECT",2]                                       |
      | ["FT.SEARCH","idx","safe","RETURN",1,"title","UNKNOWN"]                                           |
      | ["FT.AGGREGATE","idx","*","GROUPBY",0,"REDUCE","UNKNOWN",0,"AS","n"]                           |
      | ["FT.CREATE","idx","SCHEMA","title","TEXT"]                                                       |
      | ["FT.CREATE","broken","SCHEMA","v","VECTOR","FLAT",5,"TYPE","FLOAT32","DIM",2,"DISTANCE_METRIC"] |
      | ["FT.HYBRID","idx","SEARCH","safe","VSIM","v","$q","PARAMS",2,"q","bad"]                         |

  @command_FT_AGGREGATE
  Scenario: COLLECT preserves nested stage-local records across chained grouping
    Given a fresh ffkv server
    Given client "aggregate" uses RESP 3
    And the following commands succeed:
      """
      [["FT.CREATE","idx","PREFIX","1","doc:","SCHEMA","name","TEXT","SORTABLE","color","TAG","SORTABLE","price","NUMERIC","SORTABLE"],["HSET","doc:1","name","apple","color","red","price","4"],["HSET","doc:2","name","strawberry","color","red","price","3"],["HSET","doc:3","name","banana","color","yellow","price","4"],["HSET","doc:4","name","lemon","color","yellow","price","2"]]
      """
    When client "aggregate" executes:
      """
      ["FT.AGGREGATE","idx","*","GROUPBY","1","@color","REDUCE","COUNT","0","AS","cnt","REDUCE","COLLECT","4","FIELDS","2","@name","@price","AS","items","GROUPBY","0","REDUCE","COLLECT","5","FIELDS","3","@color","@cnt","@items","AS","groups"]
      """
    Then the reply contains:
      """
      {"total_results":1,"results":[{"extra_attributes":{"groups":[{"color":"red","cnt":"2","items":[{"name":"apple","price":"4"},{"name":"strawberry","price":"3"}]},{"color":"yellow","cnt":"2","items":[{"name":"banana","price":"4"},{"name":"lemon","price":"2"}]}]}}]}
      """

  @command_FT_AGGREGATE
  Scenario: Expressions compose string arithmetic conditional and UTC date functions
    Given a fresh ffkv server
    Given client "aggregate" uses RESP 3
    And the following commands succeed:
      """
      [["FT.CREATE","idx","PREFIX","1","doc:","SCHEMA","title","TEXT"],["HSET","doc:1","title","ababa"]]
      """
    When client "aggregate" executes:
      """
      ["FT.AGGREGATE","idx","*","LOAD","1","@title","APPLY","contains(@title,'a')","AS","hits","APPLY","substr(@title,-2,-1)","AS","tail","APPLY","year(0)","AS","year","APPLY","case(@hits == 3,'yes','no')","AS","verdict"]
      """
    Then the reply contains:
      """
      {"total_results":1,"results":[{"extra_attributes":{"title":"ababa","hits":"3","tail":"ba","year":"1970","verdict":"yes"}}]}
      """

  @command_FT_SEARCH @command_FT_AGGREGATE
  Scenario: Search payloads and aggregate LOAD remain binary safe
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["FT.CREATE","idx","PREFIX","1","doc:","SCORE_FIELD","rank","PAYLOAD_FIELD","blob","SCHEMA","title","TEXT"],["HSET","doc:1","title","boots","rank","0.5","blob",{"base64":"/wA="}]]
      """
    When I execute:
      """
      ["FT.SEARCH","idx","boots","WITHSCORES","WITHPAYLOADS","SCORER","DOCSCORE","RETURN","1","title"]
      """
    Then the reply equals:
      """
      [1,"doc:1","0.5",{"type":"bytes","base64":"/wA="},["title","boots"]]
      """
    When I execute:
      """
      ["FT.AGGREGATE","idx","*","LOAD","1","@blob"]
      """
    Then the reply equals:
      """
      [1,["blob",{"type":"bytes","base64":"/wA="}]]
      """
    When I execute:
      """
      ["FT.AGGREGATE","idx","*","LOAD","1","@blob","GROUPBY","0","REDUCE","COLLECT","2","FIELDS","*","AS","records"]
      """
    Then the reply equals:
      """
      [1,["records",[["blob",{"type":"bytes","base64":"/wA="}]]]]
      """
    When I execute:
      """
      ["FT.AGGREGATE","idx","*","LOAD","3","@__key","AS","source"]
      """
    Then the reply equals:
      """
      [1,["source","doc:1"]]
      """

  @command_FT_SEARCH
  Scenario: Invalid vector blobs do not undo successful document writes
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["FT.CREATE","idx","PREFIX","1","doc:","SCHEMA","v","VECTOR","HNSW","6","TYPE","FLOAT32","DIM","2","DISTANCE_METRIC","L2"],["HSET","doc:1","v",{"base64":"AAAAAA=="}]]
      """
    When I execute:
      """
      ["HGET","doc:1","v"]
      """
    Then the reply equals:
      """
      "\u0000\u0000\u0000\u0000"
      """
    When I execute:
      """
      ["FT.SEARCH","idx","*","NOCONTENT"]
      """
    Then the reply equals:
      """
      [0]
      """
    Given the following commands succeed:
      """
      [["HSET","doc:1","v",{"base64":"AAAAAAAAAAA="}]]
      """
    When I execute:
      """
      ["FT.SEARCH","idx","*=>[KNN 1 @v $q]","PARAMS","2","q",{"base64":"AAAAAAAAAAA="},"NOCONTENT","DIALECT","2"]
      """
    Then the reply equals:
      """
      [1,"doc:1"]
      """

  @command_FT_SEARCH
  Scenario: Suffix dictionaries update when their last posting disappears
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["FT.CREATE","idx","PREFIX","1","doc:","SCHEMA","title","TEXT","WITHSUFFIXTRIE"],["HSET","doc:1","title","blueberry"],["HSET","doc:2","title","raspberry"]]
      """
    When I execute:
      """
      ["FT.SEARCH","idx","*berry","NOCONTENT","LIMIT","0","0","DIALECT","2"]
      """
    Then the reply equals:
      """
      [2]
      """
    Given the following commands succeed:
      """
      [["DEL","doc:1"],["HSET","doc:2","title","lemon"]]
      """
    When I execute:
      """
      ["FT.SEARCH","idx","*berry","NOCONTENT","DIALECT","2"]
      """
    Then the reply equals:
      """
      [0]
      """

  @command_FT_DROPINDEX
  Scenario: Read-only document grants never authorize DROPINDEX DD deletion
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["FT.CREATE","idx","PREFIX","1","doc:","SCHEMA","title","TEXT"],["HSET","doc:1","title","boots"],["ACL","SETUSER","reader","on",">search-password","+@all","%R~doc:*"]]
      """
    When client "reader" executes:
      """
      ["AUTH","reader","search-password"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "reader" executes:
      """
      ["FT.DROPINDEX","idx","DD"]
      """
    Then the reply is an error with code "NOPERM"
    When I execute:
      """
      ["FT.SEARCH","idx","boots","NOCONTENT"]
      """
    Then the reply equals:
      """
      [1,"doc:1"]
      """

  @command_FT_SEARCH
  Scenario: Matching a literal prefix probe does not authorize a whole index namespace
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["FT.CREATE","idx","PREFIX","1","doc:","SCHEMA","title","TEXT"],["HSET","doc:1","title","boots"],["ACL","SETUSER","one-byte","on",">search-password","+@all","~doc:?"]]
      """
    When client "restricted" executes:
      """
      ["AUTH","one-byte","search-password"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "restricted" executes:
      """
      ["FT.SEARCH","idx","boots","NOCONTENT"]
      """
    Then the reply is an error with code "NOPERM"

  @command_FT_CONFIG @command_FT_SEARCH
  Scenario: Search worker reconfiguration preserves queries and enforces strict timeout prerequisites
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["FT.CREATE","idx","PREFIX","1","doc:","SCHEMA","title","TEXT"],["HSET","doc:1","title","workers"],["FT.CONFIG","SET","WORKERS","2"],["FT.CONFIG","SET","ON_TIMEOUT","RETURN_STRICT"]]
      """
    When I execute:
      """
      ["FT.SEARCH","idx","workers","NOCONTENT"]
      """
    Then the reply equals:
      """
      [1,"doc:1"]
      """
    When I execute:
      """
      ["FT.CONFIG","SET","WORKERS","0"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["FT.CONFIG","GET","WORKERS"]
      """
    Then the reply equals:
      """
      [["WORKERS","2"]]
      """
    Given the following commands succeed:
      """
      [["FT.CONFIG","SET","ON_TIMEOUT","RETURN"],["FT.CONFIG","SET","WORKERS","0"]]
      """
    When I execute:
      """
      ["FT.SEARCH","idx","workers","NOCONTENT"]
      """
    Then the reply equals:
      """
      [1,"doc:1"]
      """

  @command_FT_CONFIG
  Scenario: Collector scheduling controls retain their values and reject invalid intervals
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["FT.CONFIG","SET","FORK_GC_CLEAN_THRESHOLD","2"],["FT.CONFIG","SET","FORK_GC_RUN_INTERVAL","1"],["FT.CONFIG","SET","FORK_GC_RETRY_INTERVAL","3"],["FT.CONFIG","SET","FORKGC_SLEEP_BEFORE_EXIT","0"]]
      """
    When I execute:
      """
      ["FT.CONFIG","GET","FORK_GC_RETRY_INTERVAL"]
      """
    Then the reply equals:
      """
      [["FORK_GC_RETRY_INTERVAL","3"]]
      """
    When I execute:
      """
      ["FT.CONFIG","SET","FORK_GC_RUN_INTERVAL","0"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["FT.CONFIG","GET","FORK_GC_RUN_INTERVAL"]
      """
    Then the reply equals:
      """
      [["FORK_GC_RUN_INTERVAL","1"]]
      """

  @command_SCRIPT_DEBUG @command_FT_SEARCH @command_FT_ALTER @command_FT_ALIASADD @command_FT_DICTADD
  Scenario: Disposable Lua debugging sees staged search changes without publishing catalog changes
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["FT.CREATE","idx","PREFIX","1","doc:","SCHEMA","title","TEXT"],["HSET","doc:1","title","before"],["FT.DICTADD","words","before"]]
      """
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
      ["EVAL","redis.call('HSET','doc:1','title','after')\nredis.call('FT.ALIASADD','staged','idx')\nredis.call('FT.DICTADD','words','after')\nredis.call('FT.ALTER','idx','SCHEMA','ADD','tag','TAG')\nreturn {redis.call('FT.SEARCH','staged','after','NOCONTENT')[1],redis.call('FT.DICTDUMP','words')}","1","doc:1"]
      """
    Then client "debugger" receives:
      """
      ["* Stopped at 1","-> 1 redis.call('HSET','doc:1','title','after')"]
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
      [1,["after","before"]]
      """
    When I execute:
      """
      ["FT.SEARCH","idx","before","NOCONTENT"]
      """
    Then the reply equals:
      """
      [1,"doc:1"]
      """
    When I execute:
      """
      ["FT.SEARCH","staged","after","NOCONTENT"]
      """
    Then the reply is an error with code "ERR"
    When I execute:
      """
      ["FT.DICTDUMP","words"]
      """
    Then the reply equals:
      """
      ["before"]
      """
    When I execute:
      """
      ["FT.ALTER","idx","SCHEMA","ADD","tag","TAG"]
      """
    Then the reply equals:
      """
      "OK"
      """

  @command_SWAPDB @command_FT_SEARCH @command_FT_ALIASADD
  Scenario: Swapping database zero reconciles search documents without replacing schemas or aliases
    Given a fresh ffkv server
    And the following commands succeed:
      """
      [["FT.CREATE","idx","PREFIX","1","doc:","SCHEMA","title","TEXT"],["FT.ALIASADD","view","idx"],["HSET","doc:old","title","before"],["HSET","doc:shared","title","before"],["SELECT","1"],["HSET","doc:new","title","after"],["HSET","doc:shared","title","after"],["SELECT","2"],["SET","selected","database-two"],["SWAPDB","0","1"]]
      """
    When I execute:
      """
      ["GET","selected"]
      """
    Then the reply equals:
      """
      "database-two"
      """
    Given the following commands succeed:
      """
      [["SELECT","0"]]
      """
    When I execute:
      """
      ["FT.SEARCH","view","after","NOCONTENT","LIMIT","0","0"]
      """
    Then the reply equals:
      """
      [2]
      """
    When I execute:
      """
      ["FT.SEARCH","view","before","NOCONTENT"]
      """
    Then the reply equals:
      """
      [0]
      """
    Given the following commands succeed:
      """
      [["SWAPDB","1","2"]]
      """
    When I execute:
      """
      ["FT.SEARCH","view","after","NOCONTENT","LIMIT","0","0"]
      """
    Then the reply equals:
      """
      [2]
      """
    When ffkv restarts after a crash
    When I execute:
      """
      ["FT.SEARCH","view","after","NOCONTENT","LIMIT","0","0"]
      """
    Then the reply equals:
      """
      [2]
      """
    Given the following commands succeed:
      """
      [["SELECT","2"]]
      """
    When I execute:
      """
      ["HGET","doc:old","title"]
      """
    Then the reply equals:
      """
      "before"
      """
