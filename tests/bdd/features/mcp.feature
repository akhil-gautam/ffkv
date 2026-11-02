@mcp
Feature: Native ffkv MCP exposes real owned server behavior over stdio
  A persistent MCP process transports typed commands, streams and management
  operations. Each scenario owns isolated processes and temporary data.

  Scenario Outline: MCP feature contracts assert observable state
    Given an isolated real ffkv MCP fixture
    When the MCP "<group>" behavior contract is exercised
    Then the MCP "<group>" contract has passed with real replies

    Examples:
      | group          |
      | protocol       |
      | datatypes      |
      | jobs           |
      | sessions       |
      | streams        |
      | errors         |
      | administration |
      | management     |
      | artifacts      |
      | benchmark      |
      | process        |
