Feature: Redis 8.10 cluster topology, routing and real node handoffs
  # Fixtures own every process, data directory, peer socket and client connection.
  # Peer transport is ffkv-owned; these scenarios do not assert Redis bus compatibility.

  @command_CLUSTER_KEYSLOT
  Scenario: CRC16 XMODEM and nonempty first hash tags determine external slots
    Given a fresh unassigned ffkv cluster with 2 nodes
    When node "node1" executes:
      """
      ["CLUSTER","KEYSLOT","123456789"]
      """
    Then the reply equals:
      """
      12739
      """
    When node "node2" executes:
      """
      ["CLUSTER","KEYSLOT","foo{bar}{zap}"]
      """
    Then the reply equals:
      """
      5061
      """
    When node "node1" executes:
      """
      ["CLUSTER","KEYSLOT","foo{}{bar}"]
      """
    Then the reply does not equal the slot of "bar"
    When node "node2" executes:
      """
      ["CLUSTER","KEYSLOT",{"base64":"YQB7YmFyfQ=="}]
      """
    Then the reply equals:
      """
      5061
      """

  @command_CLUSTER_MYID @command_CLUSTER_MYSHARDID @command_CLUSTER_SAVECONFIG
  Scenario: Node and shard identities survive fsynced configuration and a crash
    Given a fresh ffkv cluster with 3 primaries and 3 replicas
    When node "node1" executes:
      """
      ["CLUSTER","MYID"]
      """
    Then the reply is a 40 character hexadecimal identifier
    When I save the reply as "cluster-id"
    When node "node1" executes:
      """
      ["CLUSTER","MYSHARDID"]
      """
    When I save the reply as "shard-id"
    When node "node4" executes:
      """
      ["CLUSTER","MYSHARDID"]
      """
    Then the reply equals saved "shard-id"
    When node "node1" executes:
      """
      ["CLUSTER","SAVECONFIG"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When cluster node "node1" restarts after a crash
    When node "node1" executes:
      """
      ["CLUSTER","MYID"]
      """
    Then the reply equals saved "cluster-id"
    When node "node1" executes:
      """
      ["CLUSTER","MYSHARDID"]
      """
    Then the reply equals saved "shard-id"
    Then cluster node "node1" eventually returns:
      """
      {"command":["SET","{{node1_key}}","after-restart"],"reply":"OK"}
      """

  @command_CLUSTER_MEET @command_CLUSTER_LINKS @command_CLUSTER_NODES @command_CLUSTER_INFO
  Scenario: Real peer links and gossip expose a completely covered cluster
    Given a fresh ffkv cluster with 3 primaries and 3 replicas
    When node "node1" executes:
      """
      ["CLUSTER","MEET","127.0.0.1","{{node2_port}}","{{node2_bus_port}}"]
      """
    Then the reply equals:
      """
      "OK"
      """
    Then cluster node "node1" has 6 known nodes
    When node "node1" executes:
      """
      ["CLUSTER","LINKS"]
      """
    Then the reply has a real cluster link to "node2"
    When node "node1" executes:
      """
      ["CLUSTER","NODES"]
      """
    Then the reply describes exactly one myself node
    When node "node1" executes:
      """
      ["CLUSTER","INFO"]
      """
    Then the reply contains text "cluster_state:ok"
    Then the reply contains text "cluster_slots_assigned:16384"
    Then the reply contains text "cluster_known_nodes:6"
    When node "node1" executes:
      """
      ["CLUSTER","MEET","not-an-ip","1234"]
      """
    Then the reply is an error with code "ERR"

  @command_CLUSTER_SLOTS @command_CLUSTER_SHARDS
  Scenario: Both topology APIs describe actual owners and replica groups
    Given a fresh ffkv cluster with 3 primaries and 3 replicas
    When node "node2" executes:
      """
      ["CLUSTER","SLOTS"]
      """
    Then the reply covers all 16384 slots exactly once with the fixture primaries
    Given cluster node "node3" uses RESP 3
    When node "node3" executes:
      """
      ["CLUSTER","SHARDS"]
      """
    Then the reply groups the fixture primaries with their actual replicas

  @command_CLUSTER_ADDSLOTS @command_CLUSTER_DELSLOTS
  Scenario: Slot list updates validate the whole request before mutating
    Given a fresh unassigned ffkv cluster with 2 nodes
    When node "node1" executes:
      """
      ["CLUSTER","ADDSLOTS","1","2"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When node "node1" executes:
      """
      ["SET","{{slot1_key}}","owned"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When node "node1" executes:
      """
      ["CLUSTER","ADDSLOTS","3","2"]
      """
    Then the reply is an error with code "ERR"
    When node "node1" executes:
      """
      ["SET","{{slot3_key}}","not-owned"]
      """
    Then the reply is an error with code "CLUSTERDOWN"
    When node "node1" executes:
      """
      ["CLUSTER","DELSLOTS","1","3"]
      """
    Then the reply is an error with code "ERR"
    When node "node1" executes:
      """
      ["GET","{{slot1_key}}"]
      """
    Then the reply equals:
      """
      "owned"
      """
    When node "node1" executes:
      """
      ["CLUSTER","DELSLOTS","2"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When node "node1" executes:
      """
      ["GET","{{slot2_key}}"]
      """
    Then the reply is an error with code "CLUSTERDOWN"

  @command_CLUSTER_ADDSLOTSRANGE @command_CLUSTER_DELSLOTSRANGE
  Scenario: Inclusive range endpoints and overlapping batches are atomic
    Given a fresh unassigned ffkv cluster with 2 nodes
    When node "node1" executes:
      """
      ["CLUSTER","ADDSLOTSRANGE","1","3","8","9"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When node "node1" executes:
      """
      ["SET","{{slot3_key}}","endpoint"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When node "node1" executes:
      """
      ["CLUSTER","DELSLOTSRANGE","1","2","2","3"]
      """
    Then the reply is an error with code "ERR"
    When node "node1" executes:
      """
      ["GET","{{slot3_key}}"]
      """
    Then the reply equals:
      """
      "endpoint"
      """
    When node "node1" executes:
      """
      ["CLUSTER","DELSLOTSRANGE","8","9"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When node "node1" executes:
      """
      ["CLUSTER","ADDSLOTSRANGE","5","4"]
      """
    Then the reply is an error with code "ERR"

  @command_CLUSTER_COUNTKEYSINSLOT @command_CLUSTER_GETKEYSINSLOT
  Scenario: Local slot key enumeration is bounded and excludes other slots
    Given a fresh ffkv cluster with 3 primaries and 3 replicas
    When node "node1" executes:
      """
      ["MSET","{{node1_key}}:a","A","{{node1_key}}:b","B"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When node "node1" executes:
      """
      ["CLUSTER","COUNTKEYSINSLOT","{{node1_slot}}"]
      """
    Then the reply equals:
      """
      2
      """
    When node "node1" executes:
      """
      ["CLUSTER","GETKEYSINSLOT","{{node1_slot}}","1"]
      """
    Then the reply has 1 item selected from:
      """
      ["{{node1_key}}:a","{{node1_key}}:b"]
      """
    When node "node1" executes:
      """
      ["CLUSTER","GETKEYSINSLOT","{{node1_slot}}","0"]
      """
    Then the reply equals:
      """
      []
      """
    When node "node2" executes:
      """
      ["CLUSTER","COUNTKEYSINSLOT","{{node1_slot}}"]
      """
    Then the reply equals:
      """
      0
      """
    When node "node1" executes:
      """
      ["CLUSTER","GETKEYSINSLOT","16384","1"]
      """
    Then the reply is an error with code "ERR"

  @command_CLUSTER_REPLICATE @command_CLUSTER_REPLICAS @command_CLUSTER_SLAVES
  Scenario: Cluster attachment uses real native replication and both replica spellings
    Given a fresh ffkv cluster with 3 primaries and 3 replicas
    When node "node1" executes:
      """
      ["SET","{{node1_key}}","replicated"]
      """
    Then the reply equals:
      """
      "OK"
      """
    Then cluster node "node4" eventually returns:
      """
      {"command":["CLUSTER","COUNTKEYSINSLOT","{{node1_slot}}"],"reply":1}
      """
    When node "node4" executes:
      """
      ["CLUSTER","REPLICATE","{{node1_id}}"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When node "node1" executes:
      """
      ["CLUSTER","REPLICAS","{{node1_id}}"]
      """
    Then the reply lists cluster replica "node4"
    When node "node2" executes:
      """
      ["CLUSTER","SLAVES","{{node1_id}}"]
      """
    Then the reply lists cluster replica "node4"
    When node "node1" executes:
      """
      ["CLUSTER","REPLICATE","{{node4_id}}"]
      """
    Then the reply is an error with code "ERR"

  @command_READONLY @command_READWRITE
  Scenario: Read routing permission is connection-local and never permits replica writes
    Given a fresh ffkv cluster with 3 primaries and 3 replicas
    When node "node1" executes:
      """
      ["SET","{{node1_key}}","replica-value"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When node "node4" executes:
      """
      ["GET","{{node1_key}}"]
      """
    Then the reply is an error with code "MOVED"
    When node "node4" executes:
      """
      ["READONLY"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "unprivileged" on node "node4" executes:
      """
      ["GET","{{node1_key}}"]
      """
    Then the reply is an error with code "MOVED"
    Then cluster node "node4" eventually returns:
      """
      {"command":["GET","{{node1_key}}"],"reply":"replica-value"}
      """
    When node "node4" executes:
      """
      ["SET","{{node1_key}}","forbidden"]
      """
    Then the reply is an error with code "MOVED"
    When node "node4" executes:
      """
      ["GET","{{node2_key}}"]
      """
    Then the reply is an error with code "MOVED"
    When node "node4" executes:
      """
      ["READWRITE"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When node "node4" executes:
      """
      ["GET","{{node1_key}}"]
      """
    Then the reply is an error with code "MOVED"

  @command_ASKING @command_CLUSTER_SETSLOT
  Scenario: Legacy importing grants ASKING for exactly one command on one connection
    Given a fresh ffkv cluster with 3 primaries and 3 replicas
    When node "node2" executes:
      """
      ["CLUSTER","SETSLOT","{{node1_slot}}","IMPORTING","{{node1_id}}"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When node "node1" executes:
      """
      ["CLUSTER","SETSLOT","{{node1_slot}}","MIGRATING","{{node2_id}}"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When node "node1" executes:
      """
      ["GET","{{node1_key}}"]
      """
    Then the reply is an error with code "ASK"
    When node "node2" executes:
      """
      ["GET","{{node1_key}}"]
      """
    Then the reply is an error with code "MOVED"
    When node "node2" executes:
      """
      ["ASKING"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When client "unprivileged" on node "node2" executes:
      """
      ["GET","{{node1_key}}"]
      """
    Then the reply is an error with code "MOVED"
    When node "node2" executes:
      """
      ["SET","{{node1_key}}","imported"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When node "node2" executes:
      """
      ["GET","{{node1_key}}"]
      """
    Then the reply is an error with code "MOVED"
    When node "node2" executes:
      """
      ["CLUSTER","SETSLOT","{{node1_slot}}","NODE","{{node2_id}}"]
      """
    Then the reply equals:
      """
      "OK"
      """
    Then the cluster eventually agrees on slot {{node1_slot}} owned by "node2"
    When node "node2" executes:
      """
      ["GET","{{node1_key}}"]
      """
    Then the reply equals:
      """
      "imported"
      """
    When node "node1" executes:
      """
      ["CLUSTER","SETSLOT","{{node1_slot}}","STABLE"]
      """
    Then the reply equals:
      """
      "OK"
      """

  @command_CLUSTER_SETSLOT
  Scenario: A mixed-present multi-key migration request returns TRYAGAIN without mutation
    Given a fresh ffkv cluster with 3 primaries and 3 replicas
    When node "node1" executes:
      """
      ["SET","{{node1_key}}:present","keep"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When node "node1" executes:
      """
      ["CLUSTER","SETSLOT","{{node1_slot}}","MIGRATING","{{node2_id}}"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When node "node1" executes:
      """
      ["MGET","{{node1_key}}:present","{{node1_key}}:missing"]
      """
    Then the reply is an error with code "TRYAGAIN"
    When node "node1" executes:
      """
      ["MSET","{{node1_key}}","one","{{node2_key}}","two"]
      """
    Then the reply is an error with code "CROSSSLOT"
    When node "node1" executes:
      """
      ["GET","{{node1_key}}:present"]
      """
    Then the reply equals:
      """
      "keep"
      """

  @command_CLUSTER_MIGRATION
  Scenario: Atomic import transfers real typed data and survives destination restart
    Given a fresh ffkv cluster with 3 primaries and 3 replicas
    When node "node1" executes:
      """
      ["RPUSH","{{node1_key}}:list","a","b"]
      """
    Then the reply equals:
      """
      2
      """
    When node "node1" executes:
      """
      ["HSET","{{node1_key}}:hash","field","value"]
      """
    Then the reply equals:
      """
      1
      """
    When node "node2" executes:
      """
      ["CLUSTER","MIGRATION","IMPORT","{{node1_slot}}","{{node1_slot}}"]
      """
    When I save the reply as "migration-id"
    Then cluster node "node2" migration "migration-id" eventually has state "completed"
    Then the cluster eventually agrees on slot {{node1_slot}} owned by "node2"
    When node "node2" executes:
      """
      ["LRANGE","{{node1_key}}:list","0","-1"]
      """
    Then the reply equals:
      """
      ["a","b"]
      """
    When node "node2" executes:
      """
      ["HGET","{{node1_key}}:hash","field"]
      """
    Then the reply equals:
      """
      "value"
      """
    When node "node1" executes:
      """
      ["CLUSTER","COUNTKEYSINSLOT","{{node1_slot}}"]
      """
    Then the reply equals:
      """
      0
      """
    When cluster node "node2" restarts after a crash
    Then cluster node "node2" eventually returns:
      """
      {"command":["HGET","{{node1_key}}:hash","field"],"reply":"value"}
      """
    When node "node2" executes:
      """
      ["CLUSTER","MIGRATION","STATUS"]
      """
    Then the reply is an error with code "ERR"
    When node "node2" executes:
      """
      ["CLUSTER","MIGRATION","STATUS","ID","unknown"]
      """
    Then the reply equals:
      """
      []
      """

  @command_CLUSTER_MIGRATION
  Scenario: Concurrent writers follow the actual slot handoff without losing the last acknowledged value
    Given a fresh ffkv cluster with 3 primaries and 3 replicas
    Given cluster writer "updates" on "node1" writes key "{{node1_key}}"
    Given the cluster bus pauses "node1" snapshot chunks
    When node "node2" executes:
      """
      ["CLUSTER","MIGRATION","IMPORT","{{node1_slot}}","{{node1_slot}}"]
      """
    When I save the reply as "migration-id"
    Then the cluster bus snapshot from "node1" is paused
    When cluster writer "updates" continues
    Then cluster writer "updates" acknowledges 3 more writes
    When the cluster bus releases "node1" snapshot chunks
    Then cluster node "node2" migration "migration-id" eventually has state "completed"
    When cluster writer "updates" stops
    Then cluster writer "updates" last value is on "node2"

  @command_CLUSTER_MIGRATION
  Scenario: Destination cancellation interrupts a real pending source connection
    Given a fresh ffkv cluster with 3 primaries and 3 replicas
    When cluster node "node1" is suspended
    When node "node2" executes:
      """
      ["CLUSTER","MIGRATION","IMPORT","{{node1_slot}}","{{node1_slot}}"]
      """
    When I save the reply as "migration-id"
    When node "node2" executes:
      """
      ["CLUSTER","MIGRATION","CANCEL","ID",{"saved":"migration-id"}]
      """
    Then the reply equals:
      """
      1
      """
    When cluster node "node1" resumes
    Then cluster node "node2" migration "migration-id" eventually has state "canceled"
    When node "node2" executes:
      """
      ["CLUSTER","MIGRATION","CANCEL","ALL"]
      """
    Then the reply equals:
      """
      0
      """

  @command_CLUSTER_MIGRATION
  Scenario: Snapshot cancellation cleans invisible imported data and resumes source writes
    Given a fresh ffkv cluster with 3 primaries and 3 replicas
    Given cluster node "node1" contains 128 keys in slot 1 with 65536 byte values
    Given the cluster bus pauses "node1" snapshot chunks
    When node "node2" executes:
      """
      ["CLUSTER","MIGRATION","IMPORT","1","1"]
      """
    When I save the reply as "migration-id"
    Then the cluster bus snapshot from "node1" is paused
    When node "node2" executes:
      """
      ["CLUSTER","MIGRATION","CANCEL","ID",{"saved":"migration-id"}]
      """
    Then the reply equals:
      """
      1
      """
    When the cluster bus releases "node1" snapshot chunks
    Then cluster node "node2" migration "migration-id" eventually has state "canceled"
    Then cluster node "node1" eventually returns:
      """
      {"command":["SET","{{slot1_key}}","resumed"],"reply":"OK"}
      """
    When node "node2" executes:
      """
      ["CLUSTER","COUNTKEYSINSLOT","1"]
      """
    Then the reply equals:
      """
      0
      """

  @command_CLUSTER_FAILOVER
  Scenario: Coordinated replica promotion observes the processed-offset barrier
    Given a fresh ffkv cluster with 3 primaries and 3 replicas
    When node "node1" executes:
      """
      ["SET","{{node1_key}}","before-failover"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When node "node4" executes:
      """
      ["CLUSTER","FAILOVER"]
      """
    Then the reply equals:
      """
      "OK"
      """
    Then the cluster eventually agrees on slot {{node1_slot}} owned by "node4"
    Then cluster node "node4" eventually returns:
      """
      {"command":["GET","{{node1_key}}"],"reply":"before-failover"}
      """
    When node "node1" executes:
      """
      ["SET","{{node1_key}}","old-primary"]
      """
    Then the reply is an error with code "MOVED"
    When node "node4" executes:
      """
      ["CLUSTER","FAILOVER"]
      """
    Then the reply is an error with code "ERR"

  @command_CLUSTER_FAILOVER @command_CLUSTER_COUNT_FAILURE_REPORTS @cluster_auto_election
  Scenario: Automatic election retains acknowledged data when the first voting primary is unavailable
    Given a fresh ffkv cluster with 3 primaries and 3 replicas
    When the lowest-ID cluster primary is suspended after an acknowledged write
    Then surviving primaries elect its replica with the acknowledged data
    When that failed cluster primary resumes
    Then all cluster nodes retain the elected owner
  @command_CLUSTER_FAILOVER
  Scenario: FORCE requests election while the source primary is unavailable
    Given a fresh ffkv cluster with 3 primaries and 3 replicas
    When cluster node "node1" is suspended
    When node "node4" executes:
      """
      ["CLUSTER","FAILOVER","FORCE"]
      """
    Then the reply equals:
      """
      "OK"
      """
    Then the cluster eventually agrees on slot {{node1_slot}} owned by "node4" among running nodes
    When cluster node "node1" resumes
    Then the cluster eventually agrees on slot {{node1_slot}} owned by "node4"


  @command_CLUSTER_FAILOVER
  Scenario: TAKEOVER can claim slots without available voters
    Given a fresh ffkv cluster with 3 primaries and 3 replicas
    When cluster node "node1" is suspended
    When cluster node "node2" is suspended
    When cluster node "node3" is suspended
    When node "node4" executes:
      """
      ["CLUSTER","FAILOVER","TAKEOVER"]
      """
    Then the reply equals:
      """
      "OK"
      """
    Then cluster node "node4" owns the former slots of "node1"

  @command_CLUSTER_SET_CONFIG_EPOCH @command_CLUSTER_BUMPEPOCH
  Scenario: Fresh-node config epochs and bump results persist independently of key shards
    Given a fresh isolated cluster-enabled ffkv node
    When node "node1" executes:
      """
      ["CLUSTER","SET-CONFIG-EPOCH","17"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When node "node1" executes:
      """
      ["CLUSTER","SET-CONFIG-EPOCH","18"]
      """
    Then the reply is an error with code "ERR"
    When node "node1" executes:
      """
      ["CLUSTER","BUMPEPOCH"]
      """
    Then the reply equals:
      """
      "STILL 17"
      """
    When cluster node "node1" restarts after a crash
    When node "node1" executes:
      """
      ["CLUSTER","INFO"]
      """
    Then the reply contains text "cluster_my_epoch:17"

  @command_CLUSTER_FLUSHSLOTS
  Scenario: Flushing assignments rejects a nonempty database and persists an empty release
    Given a fresh unassigned ffkv cluster with 2 nodes
    When node "node1" executes:
      """
      ["CLUSTER","ADDSLOTS","1"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When node "node1" executes:
      """
      ["SET","{{slot1_key}}","retained"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When node "node1" executes:
      """
      ["CLUSTER","FLUSHSLOTS"]
      """
    Then the reply is an error with code "ERR"
    When node "node1" executes:
      """
      ["DEL","{{slot1_key}}"]
      """
    Then the reply equals:
      """
      1
      """
    When node "node1" executes:
      """
      ["CLUSTER","FLUSHSLOTS"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When cluster node "node1" restarts after a crash
    When node "node1" executes:
      """
      ["CLUSTER","SLOTS"]
      """
    Then the reply equals:
      """
      []
      """

  @command_CLUSTER_FORGET
  Scenario: Forgetting a spare peer survives gossip and temporary blacklist retries
    Given a fresh unassigned ffkv cluster with 2 nodes
    When node "node1" executes:
      """
      ["CLUSTER","FORGET","{{node2_id}}"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When node "node1" executes:
      """
      ["CLUSTER","FORGET","{{node2_id}}"]
      """
    Then the reply equals:
      """
      "OK"
      """
    Then cluster node "node1" has 1 known nodes
    When node "node1" executes:
      """
      ["CLUSTER","FORGET","{{node1_id}}"]
      """
    Then the reply is an error with code "ERR"

  @command_CLUSTER_RESET
  Scenario: Soft reset preserves identity while hard reset regenerates it durably
    Given a fresh unassigned ffkv cluster with 2 nodes
    When node "node1" executes:
      """
      ["CLUSTER","MYID"]
      """
    When I save the reply as "original-id"
    When node "node1" executes:
      """
      ["CLUSTER","RESET","SOFT"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When node "node1" executes:
      """
      ["CLUSTER","MYID"]
      """
    Then the reply equals saved "original-id"
    When node "node1" executes:
      """
      ["CLUSTER","RESET","HARD"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When node "node1" executes:
      """
      ["CLUSTER","MYID"]
      """
    Then the reply differs from saved "original-id"
    When I save the reply as "reset-id"
    When cluster node "node1" restarts after a crash
    When node "node1" executes:
      """
      ["CLUSTER","MYID"]
      """
    Then the reply equals saved "reset-id"

  @command_CLUSTER_SLOT_STATS
  Scenario: Slot statistics use actual key counts with stable ordering and strict filters
    Given a fresh unassigned ffkv cluster with 2 nodes
    When node "node1" executes:
      """
      ["CLUSTER","ADDSLOTSRANGE","1","3"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When node "node1" executes:
      """
      ["SET","{{slot2_key}}","counted"]
      """
    Then the reply equals:
      """
      "OK"
      """
    Given cluster node "node1" uses RESP 3
    When node "node1" executes:
      """
      ["CLUSTER","SLOT-STATS","ORDERBY","key-count","LIMIT","1","DESC"]
      """
    Then the reply equals:
      """
      [[2,{"key-count":1}]]
      """
    When node "node1" executes:
      """
      ["CLUSTER","SLOT-STATS","SLOTSRANGE","1","3"]
      """
    Then the reply equals:
      """
      [[1,{"key-count":0}],[2,{"key-count":1}],[3,{"key-count":0}]]
      """
    When node "node1" executes:
      """
      ["CLUSTER","SLOT-STATS","ORDERBY","key-count","LIMIT","1","LIMIT","2"]
      """
    Then the reply is an error with code "ERR"
    When node "node1" executes:
      """
      ["CLUSTER","SLOT-STATS","ORDERBY","cpu-usec"]
      """
    Then the reply is an error with code "ERR"

  @command_CLUSTER_SLOT_STATS
  Scenario: Enabled accounting records actual request CPU memory and network work
    Given a fresh ffkv cluster with slot statistics enabled
    When node "node1" executes:
      """
      ["SET","{{node1_key}}","measured-value"]
      """
    Then the reply equals:
      """
      "OK"
      """
    Given cluster node "node1" uses RESP 3
    When node "node1" executes:
      """
      ["CLUSTER","SLOT-STATS","SLOTSRANGE","{{node1_slot}}","{{node1_slot}}"]
      """
    Then the reply has positive slot metric "network-bytes-in" for slot {{node1_slot}}
    Then the reply has positive slot metric "network-bytes-out" for slot {{node1_slot}}
    Then the reply has positive slot metric "memory-bytes" for slot {{node1_slot}}

  @command_CLUSTER_BUMPEPOCH
  Scenario: Bumping a zero epoch changes durable configuration exactly once
    Given a fresh isolated cluster-enabled ffkv node
    When node "node1" executes:
      """
      ["CLUSTER","BUMPEPOCH"]
      """
    Then the reply equals:
      """
      "BUMPED 1"
      """
    When node "node1" executes:
      """
      ["CLUSTER","BUMPEPOCH"]
      """
    Then the reply equals:
      """
      "STILL 1"
      """
    When cluster node "node1" restarts after a crash
    When node "node1" executes:
      """
      ["CLUSTER","INFO"]
      """
    Then the reply contains text "cluster_my_epoch:1"

  @command_CLUSTER_SAVECONFIG
  Scenario: Configuration persistence reports a real filesystem write failure
    Given a fresh isolated cluster-enabled ffkv node
    Given the cluster configuration directory of "node1" is unwritable
    When node "node1" executes:
      """
      ["CLUSTER","SAVECONFIG"]
      """
    Then the reply is an error with code "ERR"

  @command_CLUSTER_MIGRATION
  Scenario: Canceling the live mutation stream retains the source and hides destination data
    Given a fresh ffkv cluster with 3 primaries and 3 replicas
    Given cluster writer "updates" on "node1" writes key "{{node1_key}}"
    Given the cluster bus pauses "node1" snapshot chunks
    Given the cluster bus pauses "node1" mutation chunks
    When node "node2" executes:
      """
      ["CLUSTER","MIGRATION","IMPORT","{{node1_slot}}","{{node1_slot}}"]
      """
    When I save the reply as "migration-id"
    Then the cluster bus snapshot from "node1" is paused
    When cluster writer "updates" continues
    Then cluster writer "updates" acknowledges 3 more writes
    When cluster writer "updates" stops
    When the cluster bus releases "node1" snapshot chunks
    Then the cluster bus mutation stream from "node1" is paused
    When node "node2" executes:
      """
      ["CLUSTER","MIGRATION","CANCEL","ID",{"saved":"migration-id"}]
      """
    Then the reply equals:
      """
      1
      """
    When the cluster bus releases "node1" mutation chunks
    Then cluster node "node2" migration "migration-id" eventually has state "canceled"
    Then cluster writer "updates" last value is on "node1"
    When node "node2" executes:
      """
      ["CLUSTER","COUNTKEYSINSLOT","{{node1_slot}}"]
      """
    Then the reply equals:
      """
      0
      """

  @command_CLUSTER_MIGRATION
  Scenario: Function and search metadata accompany migrated keys without replacing unrelated catalogs
    Given a fresh ffkv cluster with 3 primaries and 3 replicas
    When node "node1" executes:
      """
      ["FUNCTION","LOAD","#!lua name=slot_functions\nredis.register_function{function_name='slot_get',flags={'no-writes'},callback=function(keys) return redis.call('GET',keys[1]) end}"]
      """
    Then the reply equals:
      """
      "slot_functions"
      """
    When node "node2" executes:
      """
      ["FUNCTION","LOAD","#!lua name=destination_functions\nredis.register_function{function_name='local_answer',flags={'no-writes'},callback=function() return 42 end}"]
      """
    Then the reply equals:
      """
      "destination_functions"
      """
    When node "node1" executes:
      """
      ["SET","{{node1_key}}:value","from-function"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When node "node1" executes:
      """
      ["FT.CREATE","source_index","ON","HASH","PREFIX","1","{{node1_key}}:document","SCHEMA","title","TEXT"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When node "node1" executes:
      """
      ["HSET","{{node1_key}}:document","title","boots"]
      """
    Then the reply equals:
      """
      1
      """
    When node "node2" executes:
      """
      ["FT.CREATE","destination_index","ON","HASH","PREFIX","1","{{node2_key}}:document","SCHEMA","title","TEXT"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When node "node2" executes:
      """
      ["HSET","{{node2_key}}:document","title","local"]
      """
    Then the reply equals:
      """
      1
      """
    When node "node2" executes:
      """
      ["CLUSTER","MIGRATION","IMPORT","{{node1_slot}}","{{node1_slot}}"]
      """
    When I save the reply as "migration-id"
    Then cluster node "node2" migration "migration-id" eventually has state "completed"
    When node "node2" executes:
      """
      ["FCALL_RO","slot_get","1","{{node1_key}}:value"]
      """
    Then the reply equals:
      """
      "from-function"
      """
    When node "node2" executes:
      """
      ["FCALL_RO","local_answer","0"]
      """
    Then the reply equals:
      """
      42
      """
    When node "node2" executes:
      """
      ["FT.SEARCH","source_index","boots","NOCONTENT"]
      """
    Then the reply equals:
      """
      [1,"{{node1_key}}:document"]
      """
    When node "node2" executes:
      """
      ["FT.SEARCH","destination_index","local","NOCONTENT"]
      """
    Then the reply equals:
      """
      [1,"{{node2_key}}:document"]
      """

  @command_CLUSTER_MIGRATION
  Scenario: Live resolved mutations replay list changes deletion and absolute expiry exactly once
    Given a fresh ffkv cluster with 3 primaries and 3 replicas
    When node "node1" executes:
      """
      ["RPUSH","{{node1_key}}:list","before"]
      """
    Then the reply equals:
      """
      1
      """
    When node "node1" executes:
      """
      ["SET","{{node1_key}}:deleted","gone"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When node "node1" executes:
      """
      ["SET","{{node1_key}}:expiring","initial"]
      """
    Then the reply equals:
      """
      "OK"
      """
    Given the cluster bus pauses "node1" snapshot chunks
    When node "node2" executes:
      """
      ["CLUSTER","MIGRATION","IMPORT","{{node1_slot}}","{{node1_slot}}"]
      """
    When I save the reply as "migration-id"
    Then the cluster bus snapshot from "node1" is paused
    When node "node1" executes:
      """
      ["RPUSH","{{node1_key}}:list","during"]
      """
    Then the reply equals:
      """
      2
      """
    When node "node1" executes:
      """
      ["DEL","{{node1_key}}:deleted"]
      """
    Then the reply equals:
      """
      1
      """
    When node "node1" executes:
      """
      ["SET","{{node1_key}}:expiring","changed","PX","120000"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When the cluster bus releases "node1" snapshot chunks
    Then cluster node "node2" migration "migration-id" eventually has state "completed"
    When node "node2" executes:
      """
      ["LRANGE","{{node1_key}}:list","0","-1"]
      """
    Then the reply equals:
      """
      ["before","during"]
      """
    When node "node2" executes:
      """
      ["GET","{{node1_key}}:deleted"]
      """
    Then the reply is null
    When node "node2" executes:
      """
      ["GET","{{node1_key}}:expiring"]
      """
    Then the reply equals:
      """
      "changed"
      """
    When node "node2" executes:
      """
      ["PTTL","{{node1_key}}:expiring"]
      """
    Then the reply is a positive integer at most 120000

  @command_CLUSTER_MIGRATION
  Scenario: Import validation rejects replica destinations multiple sources and overlapping ranges
    Given a fresh ffkv cluster with 3 primaries and 3 replicas
    When node "node4" executes:
      """
      ["CLUSTER","MIGRATION","IMPORT","{{node1_slot}}","{{node1_slot}}"]
      """
    Then the reply is an error with code "ERR"
    When node "node3" executes:
      """
      ["CLUSTER","MIGRATION","IMPORT","{{node1_slot}}","{{node1_slot}}","{{node2_slot}}","{{node2_slot}}"]
      """
    Then the reply is an error with code "ERR"
    When node "node3" executes:
      """
      ["CLUSTER","MIGRATION","IMPORT","{{node1_slot}}","{{node1_slot}}","{{node1_slot}}","{{node1_slot}}"]
      """
    Then the reply is an error with code "ERR"
    When node "node3" executes:
      """
      ["CLUSTER","MIGRATION","STATUS","ALL"]
      """
    Then the reply equals:
      """
      []
      """

  @command_CLUSTER_ADDSLOTS
  Scenario Outline: Invalid integer slot syntax cannot partially bind a batch
    Given a fresh unassigned ffkv cluster with 2 nodes
    When node "node1" executes:
      """
      ["CLUSTER","ADDSLOTS","1","<invalid>"]
      """
    Then the reply is an error with code "ERR"
    When node "node1" executes:
      """
      ["CLUSTER","ADDSLOTS","1"]
      """
    Then the reply equals:
      """
      "OK"
      """

    Examples:
      | invalid              |
      | 01                   |
      | +2                   |
      | -1                   |
      | 16384                |
      | 9223372036854775808  |

  @command_CLUSTER_INFO @command_CLUSTER_NODES
  Scenario: RESP3 preserves the verbatim text type of cluster diagnostics
    Given a fresh unassigned ffkv cluster with 2 nodes
    And cluster node "node1" uses RESP 3
    When node "node1" executes:
      """
      ["CLUSTER","INFO"]
      """
    Then the reply has JSON path "type" equal to:
      """
      "verbatim"
      """
    And the reply has JSON path "format" equal to:
      """
      "txt"
      """
    When node "node1" executes:
      """
      ["CLUSTER","NODES"]
      """
    Then the reply has JSON path "type" equal to:
      """
      "verbatim"
      """
    And the reply has JSON path "format" equal to:
      """
      "txt"
      """
    And the reply contains text "myself"
    And the reply contains text "master"

  @command_EVAL @command_FCALL
  Scenario: Dynamic Lua and function keys cannot diverge from their authoritative cluster owner
    Given a fresh ffkv cluster with 3 primaries and 3 replicas
    When node "node2" executes:
      """
      ["SET","{{node2_key}}","authoritative"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When node "node1" executes:
      """
      ["GET","{{node2_key}}"]
      """
    Then the reply is an error with code "MOVED"
    When node "node1" executes:
      """
      ["EVAL","return redis.pcall('GET',ARGV[1])",0,"{{node2_key}}"]
      """
    Then the reply is an error with code "MOVED"
    When node "node1" executes:
      """
      ["EVAL","return redis.pcall('SET',ARGV[1],'misplaced')",0,"{{node2_key}}"]
      """
    Then the reply is an error with code "MOVED"
    When node "node1" executes:
      """
      ["EVAL","#!lua flags=allow-cross-slot-keys\nredis.call('SET',KEYS[1],'local'); return redis.pcall('SET',ARGV[1],'misplaced')",1,"{{node1_key}}","{{node2_key}}"]
      """
    Then the reply is an error with code "MOVED"
    When node "node1" executes:
      """
      ["EVAL","return redis.pcall('GET',KEYS[1])",1,"{{node2_key}}"]
      """
    Then the reply is an error with code "MOVED"
    When node "node1" executes:
      """
      ["EVAL","redis.call('SET',ARGV[1],'dynamic-local'); return redis.call('GET',ARGV[1])",0,"{{node1_key}}"]
      """
    Then the reply equals:
      """
      "dynamic-local"
      """
    When node "node1" executes:
      """
      ["EVAL","redis.call('SET',KEYS[1],ARGV[1]); return redis.call('GET',KEYS[1])",1,"{{node1_key}}","declared-local"]
      """
    Then the reply equals:
      """
      "declared-local"
      """
    When node "node1" executes:
      """
      ["EVAL","return redis.pcall('GET',ARGV[1])",1,"{{node1_key}}","{{slot2_key}}"]
      """
    Then the reply is an error with code "ERR"
    When node "node1" executes:
      """
      ["EVAL","#!lua flags=allow-cross-slot-keys\nredis.call('SET',ARGV[1],'other-local-slot'); return redis.call('GET',ARGV[1])",1,"{{node1_key}}","{{slot2_key}}"]
      """
    Then the reply equals:
      """
      "other-local-slot"
      """
    When node "node1" executes:
      """
      ["FUNCTION","LOAD","#!lua name=dynamic\nredis.register_function{function_name='dynamic_get',callback=function(keys,args) return redis.pcall('GET',args[1]) end,flags={'no-writes','allow-cross-slot-keys'}}\nredis.register_function{function_name='dynamic_put',callback=function(keys,args) return redis.pcall('SET',args[1],args[2]) end,flags={'allow-cross-slot-keys'}}"]
      """
    Then the reply equals:
      """
      "dynamic"
      """
    When node "node1" executes:
      """
      ["FCALL_RO","dynamic_get",0,"{{node2_key}}"]
      """
    Then the reply is an error with code "MOVED"
    When node "node1" executes:
      """
      ["FCALL","dynamic_put",0,"{{node2_key}}","misplaced"]
      """
    Then the reply is an error with code "MOVED"
    When node "node1" executes:
      """
      ["FCALL_RO","dynamic_get",0,"{{node1_key}}"]
      """
    Then the reply equals:
      """
      "declared-local"
      """
    When node "node2" executes:
      """
      ["EVAL","return redis.call('GET',ARGV[1])",0,"{{node2_key}}"]
      """
    Then the reply equals:
      """
      "authoritative"
      """

  @command_EVAL @command_ASKING @command_CLUSTER_SETSLOT
  Scenario: Dynamic script admission shares importing and migrating key visibility
    Given a fresh ffkv cluster with 3 primaries and 3 replicas
    When node "node1" executes:
      """
      ["SET","{{node1_key}}:present","retained"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When node "node2" executes:
      """
      ["CLUSTER","SETSLOT","{{node1_slot}}","IMPORTING","{{node1_id}}"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When node "node1" executes:
      """
      ["CLUSTER","SETSLOT","{{node1_slot}}","MIGRATING","{{node2_id}}"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When node "node1" executes:
      """
      ["EVAL","return redis.call('GET',ARGV[1])",0,"{{node1_key}}:present"]
      """
    Then the reply equals:
      """
      "retained"
      """
    When node "node1" executes:
      """
      ["EVAL","return redis.pcall('GET',ARGV[1])",0,"{{node1_key}}:missing"]
      """
    Then the reply is an error with code "ASK"
    When node "node1" executes:
      """
      ["EVAL","return redis.pcall('MGET',ARGV[1],ARGV[2])",0,"{{node1_key}}:present","{{node1_key}}:missing"]
      """
    Then the reply is an error with code "TRYAGAIN"
    When node "node2" executes:
      """
      ["ASKING"]
      """
    Then the reply equals:
      """
      "OK"
      """
    When node "node2" executes:
      """
      ["EVAL","redis.call('SET',ARGV[1],'imported'); return redis.call('GET',ARGV[1])",0,"{{node1_key}}"]
      """
    Then the reply equals:
      """
      "imported"
      """
    When node "node2" executes:
      """
      ["EVAL","return redis.pcall('GET',ARGV[1])",0,"{{node1_key}}"]
      """
    Then the reply is an error with code "MOVED"
