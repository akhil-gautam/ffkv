Feature: Redis bitmap operators and bitfield boundaries
  # Contract: https://redis.io/docs/latest/commands/bitop/.

@command_SETBIT @command_GETBIT @command_BITCOUNT @command_BITPOS
Scenario: Bit offsets cross byte boundaries and range units are inclusive
  Given a fresh ffkv server
  When I execute:
    """
    ["SETBIT","bits","7","1"]
    """
  Then the reply equals:
    """
    0
    """
  When I execute:
    """
    ["SETBIT","bits","8","1"]
    """
  Then the reply equals:
    """
    0
    """
  When I execute:
    """
    ["GETBIT","bits","7"]
    """
  Then the reply equals:
    """
    1
    """
  When I execute:
    """
    ["GETBIT","bits","8"]
    """
  Then the reply equals:
    """
    1
    """
  When I execute:
    """
    ["GETBIT","bits","16"]
    """
  Then the reply equals:
    """
    0
    """
  When I execute:
    """
    ["BITCOUNT","bits"]
    """
  Then the reply equals:
    """
    2
    """
  When I execute:
    """
    ["BITCOUNT","bits","7","8","BIT"]
    """
  Then the reply equals:
    """
    2
    """
  When I execute:
    """
    ["BITCOUNT","bits","1","1","BYTE"]
    """
  Then the reply equals:
    """
    1
    """
  When I execute:
    """
    ["BITCOUNT","bits","-1","-1"]
    """
  Then the reply equals:
    """
    1
    """
  When I execute:
    """
    ["BITPOS","bits","1"]
    """
  Then the reply equals:
    """
    7
    """
  When I execute:
    """
    ["BITPOS","bits","1","8","8","BIT"]
    """
  Then the reply equals:
    """
    8
    """
  When I execute:
    """
    ["BITPOS","bits","1","9","15","BIT"]
    """
  Then the reply equals:
    """
    -1
    """
  When I execute:
    """
    ["SETBIT","bits","-1","1"]
    """
  Then the reply is an error with code "ERR"
  When I execute:
    """
    ["SETBIT","bits","0","2"]
    """
  Then the reply is an error with code "ERR"
  When I execute:
    """
    ["BITCOUNT","bits"]
    """
  Then the reply equals:
    """
    2
    """

@command_GETBIT @command_BITCOUNT @command_BITPOS
Scenario: Missing bitmaps read as zero without creating a key
  Given a fresh ffkv server
  When I execute:
    """
    ["GETBIT","missing","123"]
    """
  Then the reply equals:
    """
    0
    """
  When I execute:
    """
    ["BITCOUNT","missing"]
    """
  Then the reply equals:
    """
    0
    """
  When I execute:
    """
    ["BITPOS","missing","0"]
    """
  Then the reply equals:
    """
    0
    """
  When I execute:
    """
    ["BITPOS","missing","1"]
    """
  Then the reply equals:
    """
    -1
    """
  When I execute:
    """
    ["EXISTS","missing"]
    """
  Then the reply equals:
    """
    0
    """

@command_BITFIELD @command_BITFIELD_RO
Scenario: Bitfield overflow modes and read-only commands preserve values
  Given a fresh ffkv server
  When I execute:
    """
    ["BITFIELD","f","SET","u4","6","15","GET","u4","6"]
    """
  Then the reply equals:
    """
    [0,15]
    """
  When I execute:
    """
    ["BITFIELD","f","OVERFLOW","FAIL","INCRBY","u4","6","1","GET","u4","6"]
    """
  Then the reply equals:
    """
    [null,15]
    """
  When I execute:
    """
    ["BITFIELD","f","OVERFLOW","SAT","INCRBY","u4","6","1"]
    """
  Then the reply equals:
    """
    [15]
    """
  When I execute:
    """
    ["BITFIELD","f","OVERFLOW","WRAP","INCRBY","u4","6","1"]
    """
  Then the reply equals:
    """
    [0]
    """
  When I execute:
    """
    ["BITFIELD_RO","f","GET","u4","6","GET","u4","0"]
    """
  Then the reply equals:
    """
    [0,0]
    """
  When I execute:
    """
    ["BITFIELD_RO","f","SET","u4","0","1"]
    """
  Then the reply is an error with code "ERR"
  When I execute:
    """
    ["BITFIELD_RO","missing","GET","i8","0"]
    """
  Then the reply equals:
    """
    [0]
    """
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
    ["BITFIELD","f","SET","u64","0","1"]
    """
  Then the reply is an error with code "ERR"

@command_BITOP
Scenario: BITOP AND applies zero padding and its full truth table
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["BITFIELD","x","SET","u8","0","225","SET","u8","8","128"],["BITFIELD","y","SET","u8","0","177"],["BITFIELD","z","SET","u8","0","144"]]
    """
  When I execute:
    """
    ["BITOP","AND","out","x","y","z"]
    """
  Then the reply equals:
    """
    2
    """
  When I execute:
    """
    ["BITFIELD_RO","out","GET","u8","0","GET","u8","8"]
    """
  Then the reply equals:
    """
    [128,0]
    """

@command_BITOP
Scenario: BITOP OR applies zero padding and its full truth table
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["BITFIELD","x","SET","u8","0","225","SET","u8","8","128"],["BITFIELD","y","SET","u8","0","177"],["BITFIELD","z","SET","u8","0","144"]]
    """
  When I execute:
    """
    ["BITOP","OR","out","x","y","z"]
    """
  Then the reply equals:
    """
    2
    """
  When I execute:
    """
    ["BITFIELD_RO","out","GET","u8","0","GET","u8","8"]
    """
  Then the reply equals:
    """
    [241,128]
    """

@command_BITOP
Scenario: BITOP XOR applies zero padding and its full truth table
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["BITFIELD","x","SET","u8","0","225","SET","u8","8","128"],["BITFIELD","y","SET","u8","0","177"],["BITFIELD","z","SET","u8","0","144"]]
    """
  When I execute:
    """
    ["BITOP","XOR","out","x","y","z"]
    """
  Then the reply equals:
    """
    2
    """
  When I execute:
    """
    ["BITFIELD_RO","out","GET","u8","0","GET","u8","8"]
    """
  Then the reply equals:
    """
    [192,128]
    """

@command_BITOP
Scenario: BITOP DIFF applies zero padding and its full truth table
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["BITFIELD","x","SET","u8","0","225","SET","u8","8","128"],["BITFIELD","y","SET","u8","0","177"],["BITFIELD","z","SET","u8","0","144"]]
    """
  When I execute:
    """
    ["BITOP","DIFF","out","x","y","z"]
    """
  Then the reply equals:
    """
    2
    """
  When I execute:
    """
    ["BITFIELD_RO","out","GET","u8","0","GET","u8","8"]
    """
  Then the reply equals:
    """
    [64,128]
    """

@command_BITOP
Scenario: BITOP DIFF1 applies zero padding and its full truth table
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["BITFIELD","x","SET","u8","0","225","SET","u8","8","128"],["BITFIELD","y","SET","u8","0","177"],["BITFIELD","z","SET","u8","0","144"]]
    """
  When I execute:
    """
    ["BITOP","DIFF1","out","x","y","z"]
    """
  Then the reply equals:
    """
    2
    """
  When I execute:
    """
    ["BITFIELD_RO","out","GET","u8","0","GET","u8","8"]
    """
  Then the reply equals:
    """
    [16,0]
    """

@command_BITOP
Scenario: BITOP ANDOR applies zero padding and its full truth table
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["BITFIELD","x","SET","u8","0","225","SET","u8","8","128"],["BITFIELD","y","SET","u8","0","177"],["BITFIELD","z","SET","u8","0","144"]]
    """
  When I execute:
    """
    ["BITOP","ANDOR","out","x","y","z"]
    """
  Then the reply equals:
    """
    2
    """
  When I execute:
    """
    ["BITFIELD_RO","out","GET","u8","0","GET","u8","8"]
    """
  Then the reply equals:
    """
    [161,0]
    """

@command_BITOP
Scenario: BITOP ONE applies zero padding and its full truth table
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["BITFIELD","x","SET","u8","0","225","SET","u8","8","128"],["BITFIELD","y","SET","u8","0","177"],["BITFIELD","z","SET","u8","0","144"]]
    """
  When I execute:
    """
    ["BITOP","ONE","out","x","y","z"]
    """
  Then the reply equals:
    """
    2
    """
  When I execute:
    """
    ["BITFIELD_RO","out","GET","u8","0","GET","u8","8"]
    """
  Then the reply equals:
    """
    [64,128]
    """

@command_BITOP
Scenario: NOT is unary and aliases use original source data
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["BITFIELD","x","SET","u8","0","225","SET","u8","8","128"],["BITFIELD","y","SET","u8","0","177"],["BITFIELD","z","SET","u8","0","144"]]
    """
  When I execute:
    """
    ["BITOP","NOT","x","x"]
    """
  Then the reply equals:
    """
    2
    """
  When I execute:
    """
    ["BITFIELD_RO","x","GET","u8","0","GET","u8","8"]
    """
  Then the reply equals:
    """
    [30,127]
    """
  When I execute:
    """
    ["BITOP","NOT","out","x","y"]
    """
  Then the reply is an error with code "ERR"
  When I execute:
    """
    ["BITOP","ONE","y","y","y","y"]
    """
  Then the reply equals:
    """
    1
    """
  When I execute:
    """
    ["BITCOUNT","y"]
    """
  Then the reply equals:
    """
    0
    """
  When I execute:
    """
    ["BITOP","DIFF1","y","missing"]
    """
  Then the reply equals:
    """
    0
    """
  When I execute:
    """
    ["EXISTS","y"]
    """
  Then the reply equals:
    """
    0
    """

@command_SETBIT
Scenario: SETBIT rejects non-string inputs without mutation
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["LPUSH","bad","original"],["SET","dest","keep"]]
    """
  When I execute:
    """
    ["SETBIT","bad","0","1"]
    """
  Then the reply is an error with code "WRONGTYPE"
  When I execute:
    """
    ["LINDEX","bad","0"]
    """
  Then the reply equals:
    """
    "original"
    """
  When I execute:
    """
    ["GET","dest"]
    """
  Then the reply equals:
    """
    "keep"
    """

@command_GETBIT
Scenario: GETBIT rejects non-string inputs without mutation
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["LPUSH","bad","original"],["SET","dest","keep"]]
    """
  When I execute:
    """
    ["GETBIT","bad","0"]
    """
  Then the reply is an error with code "WRONGTYPE"
  When I execute:
    """
    ["LINDEX","bad","0"]
    """
  Then the reply equals:
    """
    "original"
    """
  When I execute:
    """
    ["GET","dest"]
    """
  Then the reply equals:
    """
    "keep"
    """

@command_BITCOUNT
Scenario: BITCOUNT rejects non-string inputs without mutation
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["LPUSH","bad","original"],["SET","dest","keep"]]
    """
  When I execute:
    """
    ["BITCOUNT","bad"]
    """
  Then the reply is an error with code "WRONGTYPE"
  When I execute:
    """
    ["LINDEX","bad","0"]
    """
  Then the reply equals:
    """
    "original"
    """
  When I execute:
    """
    ["GET","dest"]
    """
  Then the reply equals:
    """
    "keep"
    """

@command_BITPOS
Scenario: BITPOS rejects non-string inputs without mutation
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["LPUSH","bad","original"],["SET","dest","keep"]]
    """
  When I execute:
    """
    ["BITPOS","bad","1"]
    """
  Then the reply is an error with code "WRONGTYPE"
  When I execute:
    """
    ["LINDEX","bad","0"]
    """
  Then the reply equals:
    """
    "original"
    """
  When I execute:
    """
    ["GET","dest"]
    """
  Then the reply equals:
    """
    "keep"
    """

@command_BITFIELD
Scenario: BITFIELD rejects non-string inputs without mutation
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["LPUSH","bad","original"],["SET","dest","keep"]]
    """
  When I execute:
    """
    ["BITFIELD","bad","SET","u8","0","1"]
    """
  Then the reply is an error with code "WRONGTYPE"
  When I execute:
    """
    ["LINDEX","bad","0"]
    """
  Then the reply equals:
    """
    "original"
    """
  When I execute:
    """
    ["GET","dest"]
    """
  Then the reply equals:
    """
    "keep"
    """

@command_BITFIELD_RO
Scenario: BITFIELD_RO rejects non-string inputs without mutation
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["LPUSH","bad","original"],["SET","dest","keep"]]
    """
  When I execute:
    """
    ["BITFIELD_RO","bad","GET","u8","0"]
    """
  Then the reply is an error with code "WRONGTYPE"
  When I execute:
    """
    ["LINDEX","bad","0"]
    """
  Then the reply equals:
    """
    "original"
    """
  When I execute:
    """
    ["GET","dest"]
    """
  Then the reply equals:
    """
    "keep"
    """

@command_BITOP
Scenario: BITOP rejects non-string inputs without mutation
  Given a fresh ffkv server
  And the following commands succeed:
    """
    [["LPUSH","bad","original"],["SET","dest","keep"]]
    """
  When I execute:
    """
    ["BITOP","ONE","dest","missing","bad"]
    """
  Then the reply is an error with code "WRONGTYPE"
  When I execute:
    """
    ["LINDEX","bad","0"]
    """
  Then the reply equals:
    """
    "original"
    """
  When I execute:
    """
    ["GET","dest"]
    """
  Then the reply equals:
    """
    "keep"
    """
