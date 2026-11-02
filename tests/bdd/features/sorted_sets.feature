Feature: Redis 8.10 sorted-set commands preserve ordered scores and resolved effects

@command_ZADD
Scenario: ZADD applies conditional changes sequentially and preserves expiry
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["ZADD","ranked","1","a","2","b","2","c","4","d"],["EXPIREAT","ranked","4102444800"]]
    """
  When I execute:
    """
    ["ZADD","ranked","XX","GT","CH","3","b","1","c"]
    """
  Then the reply equals:
    """
    1
    """
  When I execute:
    """
    ["ZRANGE","ranked","0","-1","WITHSCORES"]
    """
  Then the reply equals:
    """
    ["a","1","c","2","b","3","d","4"]
    """
  When I execute:
    """
    ["ZADD","ranked","NX","INCR","2","a"]
    """
  Then the reply equals:
    """
    null
    """
  When I execute:
    """
    ["EXPIRETIME","ranked"]
    """
  Then the reply equals:
    """
    4102444800
    """

@command_ZCARD
Scenario: Cardinality counts distinct members rather than score pairs
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["ZADD","ranked","1","a","2","b","2","c","4","d"]]
    """
  When I execute:
    """
    ["ZCARD","ranked"]
    """
  Then the reply equals:
    """
    4
    """
  When I execute:
    """
    ["ZCARD","missing"]
    """
  Then the reply equals:
    """
    0
    """

@command_ZCOUNT
Scenario: Score ranges honor strict boundaries and infinities
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["ZADD","ranked","1","a","2","b","2","c","4","d"]]
    """
  When I execute:
    """
    ["ZCOUNT","ranked","(1","2"]
    """
  Then the reply equals:
    """
    2
    """
  When I execute:
    """
    ["ZCOUNT","ranked","+inf","-inf"]
    """
  Then the reply equals:
    """
    0
    """

@command_ZINCRBY
Scenario: Increment returns its resolved score and rejects NaN without mutation
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["ZADD","ranked","inf","a"]]
    """
  When I execute:
    """
    ["ZINCRBY","ranked","-inf","a"]
    """
  Then the reply is an error with code "ERR"
  When I execute:
    """
    ["ZSCORE","ranked","a"]
    """
  Then the reply equals:
    """
    "inf"
    """
  When I execute:
    """
    ["ZINCRBY","ranked","2.5","b"]
    """
  Then the reply equals:
    """
    "2.5"
    """

@command_ZSCORE
Scenario: Individual scores preserve absent members
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["ZADD","ranked","1","a","2","b","2","c","4","d"]]
    """
  When I execute:
    """
    ["ZSCORE","ranked","b"]
    """
  Then the reply equals:
    """
    "2"
    """
  When I execute:
    """
    ["ZSCORE","ranked","missing"]
    """
  Then the reply equals:
    """
    null
    """

@command_ZMSCORE
Scenario: Multiple score lookup preserves input order and missing slots
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["ZADD","ranked","1","a","2","b","2","c","4","d"]]
    """
  When I execute:
    """
    ["ZMSCORE","ranked","d","missing","a"]
    """
  Then the reply equals:
    """
    ["4",null,"1"]
    """

@command_ZRANK
Scenario: Ranks use lexicographic tie breaks and optional score
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["ZADD","ranked","1","a","2","b","2","c","4","d"]]
    """
  When I execute:
    """
    ["ZRANK","ranked","c","WITHSCORE"]
    """
  Then the reply equals:
    """
    [2,"2"]
    """
  When I execute:
    """
    ["ZRANK","ranked","absent"]
    """
  Then the reply equals:
    """
    null
    """

@command_ZREVRANK
Scenario: Reverse ranks invert member order within tied scores
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["ZADD","ranked","1","a","2","b","2","c","4","d"]]
    """
  When I execute:
    """
    ["ZREVRANK","ranked","b","WITHSCORE"]
    """
  Then the reply equals:
    """
    [2,"2"]
    """
  When I execute:
    """
    ["ZREVRANK","ranked","absent","WITHSCORE"]
    """
  Then the reply equals:
    """
    null
    """

@command_ZRANGE
Scenario: Modern ranges support score and lex modes with reverse pagination
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["ZADD","ranked","1","a","2","b","2","c","4","d"],["ZADD","letters","0","a","0","b","0","c","0","d"]]
    """
  When I execute:
    """
    ["ZRANGE","ranked","4","(1","BYSCORE","REV","LIMIT","1","2","WITHSCORES"]
    """
  Then the reply equals:
    """
    ["c","2","b","2"]
    """
  When I execute:
    """
    ["ZRANGE","ranked","-2","-1"]
    """
  Then the reply equals:
    """
    ["c","d"]
    """
  When I execute:
    """
    ["ZRANGE","letters","[a","(d","BYLEX","LIMIT","1","-1"]
    """
  Then the reply equals:
    """
    ["b","c"]
    """

@command_ZRANGEBYSCORE
Scenario: Legacy score range returns scores with negative unlimited count
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["ZADD","ranked","1","a","2","b","2","c","4","d"]]
    """
  When I execute:
    """
    ["ZRANGEBYSCORE","ranked","(1","+inf","LIMIT","1","-1","WITHSCORES"]
    """
  Then the reply equals:
    """
    ["c","2","d","4"]
    """

@command_ZREVRANGEBYSCORE
Scenario: Legacy reverse score range reverses bounds and tie order
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["ZADD","ranked","1","a","2","b","2","c","4","d"]]
    """
  When I execute:
    """
    ["ZREVRANGEBYSCORE","ranked","4","(1","WITHSCORES","LIMIT","1","2"]
    """
  Then the reply equals:
    """
    ["c","2","b","2"]
    """

@command_ZREVRANGE
Scenario: Reverse rank range counts indexes from the reverse ordering
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["ZADD","ranked","1","a","2","b","2","c","4","d"]]
    """
  When I execute:
    """
    ["ZREVRANGE","ranked","0","1","WITHSCORES"]
    """
  Then the reply equals:
    """
    ["d","4","c","2"]
    """

@command_ZRANGEBYLEX
Scenario: Lexical ranges distinguish inclusive and exclusive prefix bounds
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["ZADD","letters","0","a","0","b","0","ba","0","c"]]
    """
  When I execute:
    """
    ["ZRANGEBYLEX","letters","[b","(c","LIMIT","0","-1"]
    """
  Then the reply equals:
    """
    ["b","ba"]
    """
  When I execute:
    """
    ["ZRANGEBYLEX","letters","+","-"]
    """
  Then the reply equals:
    """
    []
    """

@command_ZREVRANGEBYLEX
Scenario: Reverse lex range supports offset and unbounded endpoints
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["ZADD","letters","0","a","0","b","0","ba","0","c"]]
    """
  When I execute:
    """
    ["ZREVRANGEBYLEX","letters","+","[b","LIMIT","1","2"]
    """
  Then the reply equals:
    """
    ["ba","b"]
    """
  When I execute:
    """
    ["ZREVRANGEBYLEX","letters","(b","-"]
    """
  Then the reply equals:
    """
    ["a"]
    """

@command_ZLEXCOUNT
Scenario: Lex cardinality counts only the selected binary interval
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["ZADD","letters","0","a","0","b","0","ba","0","c"]]
    """
  When I execute:
    """
    ["ZLEXCOUNT","letters","(a","[ba"]
    """
  Then the reply equals:
    """
    2
    """
  When I execute:
    """
    ["ZLEXCOUNT","letters","[b","(b"]
    """
  Then the reply equals:
    """
    0
    """

@command_ZREM
Scenario: Explicit remove counts each member once and removes an empty key
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["ZADD","ranked","1","a"]]
    """
  When I execute:
    """
    ["ZREM","ranked","a","a","absent"]
    """
  Then the reply equals:
    """
    1
    """
  When I execute:
    """
    ["EXISTS","ranked"]
    """
  Then the reply equals:
    """
    0
    """

@command_ZREMRANGEBYRANK
Scenario: Rank removal resolves negative indexes before changing the indexes
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["ZADD","ranked","1","a","2","b","2","c","4","d"]]
    """
  When I execute:
    """
    ["ZREMRANGEBYRANK","ranked","-2","-1"]
    """
  Then the reply equals:
    """
    2
    """
  When I execute:
    """
    ["ZRANGE","ranked","0","-1"]
    """
  Then the reply equals:
    """
    ["a","b"]
    """

@command_ZREMRANGEBYSCORE
Scenario: Score removal leaves exclusive lower-bound members intact
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["ZADD","ranked","1","a","2","b","2","c","4","d"]]
    """
  When I execute:
    """
    ["ZREMRANGEBYSCORE","ranked","(1","2"]
    """
  Then the reply equals:
    """
    2
    """
  When I execute:
    """
    ["ZRANGE","ranked","0","-1"]
    """
  Then the reply equals:
    """
    ["a","d"]
    """

@command_ZREMRANGEBYLEX
Scenario: Lex removal preserves scores and deletes the last-member key
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["ZADD","letters","0","a","0","b","0","ba","0","c"]]
    """
  When I execute:
    """
    ["ZREMRANGEBYLEX","letters","[b","(c"]
    """
  Then the reply equals:
    """
    2
    """
  When I execute:
    """
    ["ZRANGE","letters","0","-1"]
    """
  Then the reply equals:
    """
    ["a","c"]
    """
  When I execute:
    """
    ["ZREMRANGEBYLEX","letters","-","+"]
    """
  Then the reply equals:
    """
    2
    """
  When I execute:
    """
    ["EXISTS","letters"]
    """
  Then the reply equals:
    """
    0
    """

@command_ZPOPMIN
Scenario: Minimum pop orders ties and accepts zero without consuming data
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["ZADD","ranked","1","a","2","b","2","c","4","d"]]
    """
  When I execute:
    """
    ["ZPOPMIN","ranked","0"]
    """
  Then the reply equals:
    """
    []
    """
  When I execute:
    """
    ["ZPOPMIN","ranked","3"]
    """
  Then the reply equals:
    """
    ["a","1","b","2","c","2"]
    """
  When I execute:
    """
    ["ZCARD","ranked"]
    """
  Then the reply equals:
    """
    1
    """

@command_ZPOPMAX
Scenario: Maximum pop reverses tied members and deletes the empty key
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["ZADD","ranked","1","a","2","b","2","c","4","d"]]
    """
  When I execute:
    """
    ["ZPOPMAX","ranked","8"]
    """
  Then the reply equals:
    """
    ["d","4","c","2","b","2","a","1"]
    """
  When I execute:
    """
    ["EXISTS","ranked"]
    """
  Then the reply equals:
    """
    0
    """

@command_ZRANDMEMBER
Scenario: Random members distinguish scalar and repeated scored arrays
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["ZADD","single","7","only"]]
    """
  When I execute:
    """
    ["ZRANDMEMBER","single"]
    """
  Then the reply equals:
    """
    "only"
    """
  When I execute:
    """
    ["ZRANDMEMBER","single","-3","WITHSCORES"]
    """
  Then the reply equals:
    """
    ["only","7","only","7","only","7"]
    """
  When I execute:
    """
    ["ZRANDMEMBER","single","9"]
    """
  Then the reply equals:
    """
    ["only"]
    """
  When I execute:
    """
    ["ZCARD","single"]
    """
  Then the reply equals:
    """
    1
    """

@command_ZSCAN
Scenario: Scan returns real members and decimal score text
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["ZADD","single","2.5","only"]]
    """
  When I execute:
    """
    ["ZSCAN","single","0","MATCH","on*","COUNT","100"]
    """
  Then the reply equals:
    """
    ["0",["only","2.5"]]
    """

@command_ZDIFF
Scenario: Difference retains first-source scores and repeated sources cancel
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["ZADD","left","1","a","2","b","4","c"],["ZADD","right","3","b","5","d"]]
    """
  When I execute:
    """
    ["ZDIFF","2","left","right","WITHSCORES"]
    """
  Then the reply equals:
    """
    ["a","1","c","4"]
    """
  When I execute:
    """
    ["ZDIFF","2","left","left"]
    """
  Then the reply equals:
    """
    []
    """

@command_ZDIFFSTORE
Scenario: Difference store can overwrite its source and clears destination TTL
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["ZADD","left","1","a","2","b","4","c"],["ZADD","right","3","b","5","d"],["EXPIRE","left","1000"]]
    """
  When I execute:
    """
    ["ZDIFFSTORE","left","2","left","right"]
    """
  Then the reply equals:
    """
    2
    """
  When I execute:
    """
    ["ZRANGE","left","0","-1","WITHSCORES"]
    """
  Then the reply equals:
    """
    ["a","1","c","4"]
    """
  When I execute:
    """
    ["TTL","left"]
    """
  Then the reply equals:
    """
    -1
    """

@command_ZUNION
Scenario: Union applies per-input weights before aggregation including regular sets
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["ZADD","left","1","a","2","b","4","c"],["ZADD","right","3","b","5","d"],["SADD","plain","a","d"]]
    """
  When I execute:
    """
    ["ZUNION","3","left","right","plain","WEIGHTS","2","3","4","AGGREGATE","MAX","WITHSCORES"]
    """
  Then the reply equals:
    """
    ["a","4","c","8","b","9","d","15"]
    """

@command_ZINTER
Scenario: Intersection weights and MIN aggregate preserve shared members only
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["ZADD","left","1","a","2","b","4","c"],["ZADD","right","3","b","5","d"]]
    """
  When I execute:
    """
    ["ZINTER","2","left","right","WEIGHTS","4","2","AGGREGATE","MIN","WITHSCORES"]
    """
  Then the reply equals:
    """
    ["b","6"]
    """
  When I execute:
    """
    ["ZINTER","2","left","missing"]
    """
  Then the reply equals:
    """
    []
    """

@command_ZINTERCARD
Scenario: Intersection cardinality supports regular sets duplicate keys and LIMIT
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["ZADD","left","1","a","2","b","4","c"],["ZADD","right","3","b","5","d"],["SADD","plain","a","b","c"]]
    """
  When I execute:
    """
    ["ZINTERCARD","3","left","plain","left","LIMIT","2"]
    """
  Then the reply equals:
    """
    2
    """
  When I execute:
    """
    ["ZINTERCARD","2","left","missing","LIMIT","0"]
    """
  Then the reply equals:
    """
    0
    """

@command_ZUNIONSTORE
Scenario: Union store counts weighted memberships instead of original scores
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["ZADD","left","1","a","2","b","4","c"],["ZADD","right","3","b","5","d"]]
    """
  When I execute:
    """
    ["ZUNIONSTORE","left","3","left","right","left","WEIGHTS","2","3","4","AGGREGATE","COUNT"]
    """
  Then the reply equals:
    """
    4
    """
  When I execute:
    """
    ["ZRANGE","left","0","-1","WITHSCORES"]
    """
  Then the reply equals:
    """
    ["d","3","a","6","c","6","b","9"]
    """

@command_ZINTERSTORE
Scenario: Intersection store resolves source aliasing and normalizes infinity NaN
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["ZADD","left","inf","a","2","b"],["ZADD","right","-inf","a","3","b"]]
    """
  When I execute:
    """
    ["ZINTERSTORE","left","2","left","right","AGGREGATE","SUM"]
    """
  Then the reply equals:
    """
    2
    """
  When I execute:
    """
    ["ZRANGE","left","0","-1","WITHSCORES"]
    """
  Then the reply equals:
    """
    ["a","0","b","5"]
    """

@command_ZRANGESTORE
Scenario: Range store resolves source before overwrite and preserves original scores
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["ZADD","ranked","1","a","2","b","2","c","4","d"],["EXPIRE","ranked","1000"]]
    """
  When I execute:
    """
    ["ZRANGESTORE","ranked","ranked","4","(1","BYSCORE","REV","LIMIT","0","2"]
    """
  Then the reply equals:
    """
    2
    """
  When I execute:
    """
    ["ZRANGE","ranked","0","-1","WITHSCORES"]
    """
  Then the reply equals:
    """
    ["c","2","d","4"]
    """
  When I execute:
    """
    ["TTL","ranked"]
    """
  Then the reply equals:
    """
    -1
    """
  When I execute:
    """
    ["ZRANGESTORE","ranked","missing","0","-1"]
    """
  Then the reply equals:
    """
    0
    """
  When I execute:
    """
    ["EXISTS","ranked"]
    """
  Then the reply equals:
    """
    0
    """

@command_ZMPOP
Scenario: Multi pop selects first nonempty source and returns nested pairs
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["ZADD","ranked","1","a","2","b","2","c","4","d"],["ZADD","other","-5","x"]]
    """
  When I execute:
    """
    ["ZMPOP","3","missing","ranked","other","MAX","COUNT","2"]
    """
  Then the reply equals:
    """
    ["ranked",[["d","4"],["c","2"]]]
    """
  When I execute:
    """
    ["ZSCORE","other","x"]
    """
  Then the reply equals:
    """
    "-5"
    """
  When I execute:
    """
    ["ZMPOP","1","absent","MIN"]
    """
  Then the reply equals:
    """
    null
    """

@command_BZPOPMIN
Scenario: Blocking minimum pop wakes on insertion and returns a flat scored tuple
  Given a fresh ffkv server
  When client "waiting" sends:
    """
    ["BZPOPMIN","first","second","0"]
    """
  When I execute:
    """
    ["ZADD","second","3","member"]
    """
  Then the reply equals:
    """
    1
    """
  Then client "waiting" receives:
    """
    ["second","member","3"]
    """
  When I execute:
    """
    ["EXISTS","second"]
    """
  Then the reply equals:
    """
    0
    """

@command_BZPOPMAX
Scenario: Blocking maximum pop wakes and respects reverse tied member order
  Given a fresh ffkv server
  When client "waiting" sends:
    """
    ["BZPOPMAX","ranked","0"]
    """
  When I execute:
    """
    ["ZADD","ranked","5","a","5","z"]
    """
  Then the reply equals:
    """
    2
    """
  Then client "waiting" receives:
    """
    ["ranked","z","5"]
    """
  When I execute:
    """
    ["ZRANGE","ranked","0","-1"]
    """
  Then the reply equals:
    """
    ["a"]
    """

@command_BZMPOP
Scenario: Blocking multi pop wakes and caps count at available cardinality
  Given a fresh ffkv server
  When client "waiting" sends:
    """
    ["BZMPOP","0","2","first","second","MIN","COUNT","3"]
    """
  When I execute:
    """
    ["ZADD","second","1","a","2","b"]
    """
  Then the reply equals:
    """
    2
    """
  Then client "waiting" receives:
    """
    ["second",[["a","1"],["b","2"]]]
    """
  When I execute:
    """
    ["EXISTS","second"]
    """
  Then the reply equals:
    """
    0
    """

@command_ZRANGE @command_ZPOPMIN @command_ZRANDMEMBER
Scenario: RESP3 scored ranges and pop replies use nested native doubles
  Given a fresh ffkv server
  And client "reader" uses RESP 3
  And the following commands succeed:
    """
    [["ZADD","ranked","1","a","2","b","2","c","4","d"]]
    """
  When client "reader" executes:
    """
    ["ZRANGE","ranked","0","1","WITHSCORES"]
    """
  Then the reply equals:
    """
    [["a",1],["b",2]]
    """
  When client "reader" executes:
    """
    ["ZPOPMIN","ranked","1"]
    """
  Then the reply equals:
    """
    [["a",1]]
    """
  When client "reader" executes:
    """
    ["ZRANDMEMBER","ranked","0","WITHSCORES"]
    """
  Then the reply equals:
    """
    []
    """

@command_ZINTER @command_ZUNION
Scenario: RESP3 algebra uses scored pairs and weighted COUNT ignores scores
  Given a fresh ffkv server
  And client "reader" uses RESP 3
  And the following commands succeed:
    """
    [["ZADD","left","1","a","2","b","4","c"],["ZADD","right","3","b","5","d"]]
    """
  When client "reader" executes:
    """
    ["ZINTER","2","left","right","WEIGHTS","2","3","AGGREGATE","COUNT","WITHSCORES"]
    """
  Then the reply equals:
    """
    [["b",5]]
    """
  When client "reader" executes:
    """
    ["ZUNION","1","left","WITHSCORES"]
    """
  Then the reply equals:
    """
    [["a",1],["b",2],["c",4]]
    """

@command_ZDIFFSTORE @command_ZINTERSTORE @command_ZUNIONSTORE @command_ZRANGESTORE
Scenario: Invalid store options and source types do not destroy destination
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["ZADD","left","1","a","2","b","4","c"],["ZADD","right","3","b","5","d"],["SET","keep","sentinel"],["SET","wrong","string"]]
    """
  When I execute:
    """
    ["ZUNIONSTORE","keep","1","left","WEIGHTS","nan"]
    """
  Then the reply is an error with code "ERR"
  When I execute:
    """
    ["GET","keep"]
    """
  Then the reply equals:
    """
    "sentinel"
    """
  When I execute:
    """
    ["ZINTERSTORE","keep","2","left","wrong"]
    """
  Then the reply is an error with code "WRONGTYPE"
  When I execute:
    """
    ["GET","keep"]
    """
  Then the reply equals:
    """
    "sentinel"
    """
  When I execute:
    """
    ["ZDIFFSTORE","keep","1","left","WITHSCORES"]
    """
  Then the reply is an error with code "ERR"
  When I execute:
    """
    ["GET","keep"]
    """
  Then the reply equals:
    """
    "sentinel"
    """
  When I execute:
    """
    ["ZRANGESTORE","keep","left","[a","[z","BYLEX","BYSCORE"]
    """
  Then the reply is an error with code "ERR"
  When I execute:
    """
    ["GET","keep"]
    """
  Then the reply equals:
    """
    "sentinel"
    """

@command_ZRANGE @command_ZLEXCOUNT @command_ZREMRANGEBYLEX
Scenario: Malformed range bounds and limits leave data unchanged
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["ZADD","letters","0","a","0","b","0","ba","0","c"]]
    """
  When I execute:
    """
    ["ZREMRANGEBYLEX","letters","bad","+"]
    """
  Then the reply is an error with code "ERR"
  When I execute:
    """
    ["ZLEXCOUNT","letters","-","bad"]
    """
  Then the reply is an error with code "ERR"
  When I execute:
    """
    ["ZRANGE","letters","0","2","LIMIT","0","1"]
    """
  Then the reply is an error with code "ERR"
  When I execute:
    """
    ["ZCARD","letters"]
    """
  Then the reply equals:
    """
    4
    """

@command_ZMPOP @command_BZMPOP @command_BZPOPMIN @command_BZPOPMAX
Scenario: Multi pop validates count and timeout before taking a member
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["ZADD","ranked","1","a","2","b","2","c","4","d"]]
    """
  When I execute:
    """
    ["ZMPOP","1","ranked","MIN","COUNT","0"]
    """
  Then the reply is an error with code "ERR"
  When I execute:
    """
    ["BZMPOP","-1","1","ranked","MIN"]
    """
  Then the reply is an error with code "ERR"
  When I execute:
    """
    ["BZPOPMIN","ranked","nan"]
    """
  Then the reply is an error with code "ERR"
  When I execute:
    """
    ["BZPOPMAX","ranked","-1"]
    """
  Then the reply is an error with code "ERR"
  When I execute:
    """
    ["ZCARD","ranked"]
    """
  Then the reply equals:
    """
    4
    """

@command_ZUNION
Scenario: Weight zero normalizes infinity products to zero and MIN differs from SUM
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["ZADD","left","inf","a","-inf","b"],["ZADD","right","2","a","3","b"]]
    """
  When I execute:
    """
    ["ZUNION","2","left","right","WEIGHTS","0","1","WITHSCORES"]
    """
  Then the reply equals:
    """
    ["a","2","b","3"]
    """
  When I execute:
    """
    ["ZUNION","2","left","right","AGGREGATE","MIN","WITHSCORES"]
    """
  Then the reply equals:
    """
    ["b","-inf","a","2"]
    """

@command_ZADD @command_ZREM @command_ZUNIONSTORE @command_ZMPOP
Scenario: Resolved scores stores and removals survive crash recovery
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["ZADD","left","1","a","2","b","4","c"],["ZADD","right","3","b","5","d"]]
    """
  When I execute:
    """
    ["ZUNIONSTORE","stored","2","left","right"]
    """
  Then the reply equals:
    """
    4
    """
  When I execute:
    """
    ["ZREM","stored","a"]
    """
  Then the reply equals:
    """
    1
    """
  When I execute:
    """
    ["ZMPOP","1","stored","MAX"]
    """
  Then the reply equals:
    """
    ["stored",[["d","5"]]]
    """
  When ffkv restarts after a crash
  When I execute:
    """
    ["ZRANGE","stored","0","-1","WITHSCORES"]
    """
  Then the reply equals:
    """
    ["c","4","b","5"]
    """

@command_BZPOPMIN @command_BZPOPMAX @command_BZMPOP
Scenario: Blocking sorted-set timeouts return null without creating keys
  Given a fresh ffkv server
  When client "minimum" sends:
    """
    ["BZPOPMIN","absent","0.001"]
    """
  Then client "minimum" receives:
    """
    null
    """
  When client "maximum" sends:
    """
    ["BZPOPMAX","absent","0.001"]
    """
  Then client "maximum" receives:
    """
    null
    """
  When client "many" sends:
    """
    ["BZMPOP","0.001","1","absent","MIN","COUNT","2"]
    """
  Then client "many" receives:
    """
    null
    """
  When I execute:
    """
    ["EXISTS","absent"]
    """
  Then the reply equals:
    """
    0
    """

@command_ZMPOP @command_BZMPOP @command_BZPOPMAX
Scenario: RESP3 multi-pop keeps nested pairs and blocking pop keeps its flat tuple
  Given a fresh ffkv server
  And client "reader" uses RESP 3
  And the following commands succeed:
    """
    [["ZADD","ranked","1.5","a","2.5","b","3.5","c"]]
    """
  When client "reader" executes:
    """
    ["ZMPOP","1","ranked","MIN","COUNT","1"]
    """
  Then the reply equals:
    """
    ["ranked",[["a",1.5]]]
    """
  When client "reader" executes:
    """
    ["BZMPOP","0","1","ranked","MAX","COUNT","1"]
    """
  Then the reply equals:
    """
    ["ranked",[["c",3.5]]]
    """
  When client "reader" executes:
    """
    ["BZPOPMAX","ranked","0"]
    """
  Then the reply equals:
    """
    ["ranked","b",2.5]
    """

@command_ZRANGEBYLEX @command_ZLEXCOUNT @command_ZRANGESTORE
Scenario: Lex bounds compare binary bytes and store the selected scored members
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["ZADD","binary","0",{"base64":"YQA="},"0",{"base64":"Yf8="},"0","b"]]
    """
  When I execute:
    """
    ["ZRANGEBYLEX","binary",{"base64":"W2EA"},"(b"]
    """
  Then the reply equals:
    """
    ["a\u0000",{"type":"bytes","base64":"Yf8="}]
    """
  When I execute:
    """
    ["ZLEXCOUNT","binary",{"base64":"KGEA"},"(b"]
    """
  Then the reply equals:
    """
    1
    """
  When I execute:
    """
    ["ZRANGESTORE","out","binary","[b",{"base64":"W2EA"},"BYLEX","REV","LIMIT","1","1"]
    """
  Then the reply equals:
    """
    1
    """
  When I execute:
    """
    ["ZRANGE","out","0","-1","WITHSCORES"]
    """
  Then the reply equals:
    """
    [{"type":"bytes","base64":"Yf8="},"0"]
    """

@command_ZDIFF @command_ZDIFFSTORE @command_ZINTER @command_ZUNION
Scenario: Only union and intersection accept regular set inputs
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["SADD","plain","a","b"],["ZADD","ranked","7","a"],["SET","destination","keep"]]
    """
  When I execute:
    """
    ["ZINTER","2","plain","ranked","WITHSCORES"]
    """
  Then the reply equals:
    """
    ["a","8"]
    """
  When I execute:
    """
    ["ZUNION","1","plain","WITHSCORES"]
    """
  Then the reply equals:
    """
    ["a","1","b","1"]
    """
  When I execute:
    """
    ["ZDIFF","2","ranked","plain"]
    """
  Then the reply is an error with code "WRONGTYPE"
  When I execute:
    """
    ["ZDIFFSTORE","destination","1","plain"]
    """
  Then the reply is an error with code "WRONGTYPE"
  When I execute:
    """
    ["GET","destination"]
    """
  Then the reply equals:
    """
    "keep"
    """

@command_ZINTER @command_ZINTERSTORE
Scenario Outline: Intersection preserves raw later infinity products in every aggregate and stored score
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["ZADD","finite","<finite>","a"],["ZADD","infinite","inf","a","1","filler"]]
    """
  When I execute:
    """
    ["ZINTER","2","infinite","finite","WEIGHTS","0","1","AGGREGATE","<aggregate>","WITHSCORES"]
    """
  Then the reply equals:
    """
    ["a","<expected>"]
    """
  When I execute:
    """
    ["ZINTERSTORE","finite","2","finite","infinite","WEIGHTS","1","0","AGGREGATE","<aggregate>"]
    """
  Then the reply equals:
    """
    1
    """
  When I execute:
    """
    ["ZRANGE","finite","0","-1","WITHSCORES"]
    """
  Then the reply equals:
    """
    ["a","<expected>"]
    """

  Examples:
    | finite | aggregate | expected |
    | 5      | SUM       | 0        |
    | 5      | MIN       | 5        |
    | -5     | MAX       | -5       |
    | 5      | COUNT     | 1        |

@command_ZINTER @command_ZUNION
Scenario: An initial zero-weight infinity product is normalized but union normalizes all contributions
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["ZADD","finite","5","a"],["ZADD","infinite","inf","a"]]
    """
  When I execute:
    """
    ["ZINTER","2","finite","infinite","WEIGHTS","1","0","WITHSCORES"]
    """
  Then the reply equals:
    """
    ["a","0"]
    """
  When I execute:
    """
    ["ZINTER","2","infinite","finite","WEIGHTS","0","1","WITHSCORES"]
    """
  Then the reply equals:
    """
    ["a","5"]
    """
  When I execute:
    """
    ["ZUNION","2","finite","infinite","WEIGHTS","1","0","WITHSCORES"]
    """
  Then the reply equals:
    """
    ["a","5"]
    """
  When I execute:
    """
    ["ZUNION","2","finite","infinite","WEIGHTS","1","0","AGGREGATE","MIN","WITHSCORES"]
    """
  Then the reply equals:
    """
    ["a","0"]
    """
