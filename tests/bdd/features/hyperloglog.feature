Feature: HYLL HyperLogLog cardinality and real diagnostics
  # Contracts: https://redis.io/docs/latest/commands/pfdebug/ and pfselftest/.

@command_PFADD @command_PFCOUNT
Scenario: Duplicate elements do not update cardinality and cache invalidates
  Given a fresh ffkv server
  When I execute:
    """
    ["PFADD","h"]
    """
  Then the reply equals:
    """
    1
    """
  When I execute:
    """
    ["PFCOUNT","h"]
    """
  Then the reply equals:
    """
    0
    """
  When I execute:
    """
    ["PFADD","h","a","b","c"]
    """
  Then the reply equals:
    """
    1
    """
  When I execute:
    """
    ["PFCOUNT","h"]
    """
  Then the reply equals:
    """
    3
    """
  When I execute:
    """
    ["PFADD","h","a","b","c"]
    """
  Then the reply equals:
    """
    0
    """
  When I execute:
    """
    ["PFCOUNT","h"]
    """
  Then the reply equals:
    """
    3
    """
  When I execute:
    """
    ["PFADD","h","d"]
    """
  Then the reply equals:
    """
    1
    """
  When I execute:
    """
    ["PFCOUNT","h"]
    """
  Then the reply equals:
    """
    4
    """
  When I execute:
    """
    ["PFCOUNT","missing"]
    """
  Then the reply equals:
    """
    0
    """
  When I execute:
    """
    ["EXISTS","missing"]
    """
  Then the reply equals:
    """
    0
    """

@command_PFMERGE @command_PFCOUNT
Scenario: Union deduplicates overlapping input and supports destination aliases
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["PFADD","a","a","b","c"],["PFADD","b","b","c","d"]]
    """
  When I execute:
    """
    ["PFCOUNT","a","b"]
    """
  Then the reply equals:
    """
    4
    """
  When I execute:
    """
    ["PFMERGE","a","a","b"]
    """
  Then the reply equals:
    """
    "OK"
    """
  When I execute:
    """
    ["PFCOUNT","a"]
    """
  Then the reply equals:
    """
    4
    """
  When I execute:
    """
    ["PFMERGE","a"]
    """
  Then the reply equals:
    """
    "OK"
    """
  When I execute:
    """
    ["PFCOUNT","a"]
    """
  Then the reply equals:
    """
    4
    """
  When I execute:
    """
    ["PFMERGE","empty","missing"]
    """
  Then the reply equals:
    """
    "OK"
    """
  When I execute:
    """
    ["PFCOUNT","empty"]
    """
  Then the reply equals:
    """
    0
    """
  When I execute:
    """
    ["EXISTS","empty"]
    """
  Then the reply equals:
    """
    1
    """

@command_PFDEBUG
Scenario: Sparse decoding and dense conversion expose real representation
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["SET","h",{"base64":"SFlMTAEAAAAAAAAAAAAAAH//"}]]
    """
  When I execute:
    """
    ["PFDEBUG","DECODE","h"]
    """
  Then the reply equals:
    """
    "Z:16384"
    """
  When I execute:
    """
    ["PFDEBUG","TODENSE","h"]
    """
  Then the reply equals:
    """
    1
    """
  When I execute:
    """
    ["PFDEBUG","ENCODING","h"]
    """
  Then the reply equals:
    """
    "dense"
    """
  When I execute:
    """
    ["PFDEBUG","TODENSE","h"]
    """
  Then the reply equals:
    """
    0
    """
  When I execute:
    """
    ["PFDEBUG","DECODE","h"]
    """
  Then the reply is an error with code "ERR"
  When I execute:
    """
    ["PFCOUNT","h"]
    """
  Then the reply equals:
    """
    0
    """
  When I execute:
    """
    ["PFDEBUG","GETREG","missing"]
    """
  Then the reply is an error with code "ERR"
  When I execute:
    """
    ["EXISTS","missing"]
    """
  Then the reply equals:
    """
    0
    """
  When I execute:
    """
    ["PFDEBUG","INVALID","h"]
    """
  Then the reply is an error with code "ERR"

@command_PFDEBUG
Scenario: Register diagnostics return decoded registers without changing cardinality
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["PFADD","h","foo","bar"]]
    """
  When I execute:
    """
    ["PFDEBUG","GETREG","h"]
    """
  Then the reply has 16384 items
  Then the reply contains:
    """
    [0]
    """
  When I execute:
    """
    ["PFCOUNT","h"]
    """
  Then the reply equals:
    """
    2
    """

@command_PFSELFTEST
Scenario: Seeded register and estimator diagnostics preserve database keys
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["SET","sentinel","unchanged"],["PFADD","h","a","b"]]
    """
  When I execute:
    """
    ["PFSELFTEST"]
    """
  Then the reply equals:
    """
    "OK"
    """
  When I execute:
    """
    ["GET","sentinel"]
    """
  Then the reply equals:
    """
    "unchanged"
    """
  When I execute:
    """
    ["PFCOUNT","h"]
    """
  Then the reply equals:
    """
    2
    """
  When I execute:
    """
    ["PFSELFTEST","extra"]
    """
  Then the reply is an error with code "ERR"

@command_PFADD
Scenario: PFADD rejects invalid HYLL without mutating existing values
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["SET","bad","not-a-sketch"],["PFADD","dest","a","b"]]
    """
  When I execute:
    """
    ["PFADD","bad","x"]
    """
  Then the reply is an error with code "WRONGTYPE"
  When I execute:
    """
    ["GET","bad"]
    """
  Then the reply equals:
    """
    "not-a-sketch"
    """
  When I execute:
    """
    ["PFCOUNT","dest"]
    """
  Then the reply equals:
    """
    2
    """

@command_PFCOUNT
Scenario: PFCOUNT rejects invalid HYLL without mutating existing values
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["SET","bad","not-a-sketch"],["PFADD","dest","a","b"]]
    """
  When I execute:
    """
    ["PFCOUNT","missing","bad"]
    """
  Then the reply is an error with code "WRONGTYPE"
  When I execute:
    """
    ["GET","bad"]
    """
  Then the reply equals:
    """
    "not-a-sketch"
    """
  When I execute:
    """
    ["PFCOUNT","dest"]
    """
  Then the reply equals:
    """
    2
    """

@command_PFMERGE
Scenario: PFMERGE rejects invalid HYLL without mutating existing values
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["SET","bad","not-a-sketch"],["PFADD","dest","a","b"]]
    """
  When I execute:
    """
    ["PFMERGE","dest","missing","bad"]
    """
  Then the reply is an error with code "WRONGTYPE"
  When I execute:
    """
    ["GET","bad"]
    """
  Then the reply equals:
    """
    "not-a-sketch"
    """
  When I execute:
    """
    ["PFCOUNT","dest"]
    """
  Then the reply equals:
    """
    2
    """

@command_PFDEBUG
Scenario: PFDEBUG rejects invalid HYLL without mutating existing values
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["SET","bad","not-a-sketch"],["PFADD","dest","a","b"]]
    """
  When I execute:
    """
    ["PFDEBUG","GETREG","bad"]
    """
  Then the reply is an error with code "WRONGTYPE"
  When I execute:
    """
    ["GET","bad"]
    """
  Then the reply equals:
    """
    "not-a-sketch"
    """
  When I execute:
    """
    ["PFCOUNT","dest"]
    """
  Then the reply equals:
    """
    2
    """

@command_PFCOUNT @command_PFDEBUG
Scenario: Malformed sparse runs are rejected before cache or register access
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["SET","bad",{"base64":"SFlMTAEAAAAAAAAAAAAAAH///w=="}]]
    """
  When I execute:
    """
    ["PFCOUNT","bad"]
    """
  Then the reply is an error with code "INVALIDOBJ"
  When I execute:
    """
    ["PFDEBUG","GETREG","bad"]
    """
  Then the reply is an error with code "INVALIDOBJ"
  When I execute:
    """
    ["GET","bad"]
    """
  Then the reply equals:
    """
    {"type":"bytes","base64":"SFlMTAEAAAAAAAAAAAAAAH///w=="}
    """

@command_PFMERGE @command_PFCOUNT
Scenario: Sketch merges and cache refresh retain expiry and survive recovery
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["PFADD","a","a","b"],["PFADD","b","b","c"],["PEXPIRE","a","600000"]]
    """
  When I execute:
    """
    ["PFMERGE","a","b"]
    """
  Then the reply equals:
    """
    "OK"
    """
  When I execute:
    """
    ["PFCOUNT","a"]
    """
  Then the reply equals:
    """
    3
    """
  When I execute:
    """
    ["PERSIST","a"]
    """
  Then the reply equals:
    """
    1
    """
  When ffkv restarts after a crash
  When I execute:
    """
    ["PFCOUNT","a"]
    """
  Then the reply equals:
    """
    3
    """
