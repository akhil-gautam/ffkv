@native_history
Feature: Native consumers retain authoritative source time and history
  Scenario: A follower inspects queue deadlines without originating source effects
    When an owned native follower crosses a queue deadline while its source is stopped
    Then the queued source transition applies once without replacement synchronization

  Scenario: Chained full synchronization preserves an elapsed payload awaiting PERSIST
    When an owned chained follower snapshots an elapsed list before its source PERSIST arrives
    Then every chained native consumer retains the persistent list payload

  Scenario: Equal numeric offsets cannot continue an obsolete chained image
    When an owned middle replica installs unrelated history at its downstream offset
    Then the chained consumer installs the replacement image before receiving its tail

  Scenario: Ordinary promotion preserves a legitimate chained continuation
    When an owned middle replica is promoted while its chained follower is disconnected
    Then the chained follower partially resumes the promoted history with its data intact
