#!/usr/bin/env python3
"""Compare reused MCP stdio and direct RESP with identical real operations.

No subprocess per command, synthetic timings, external servers or targets.
Reports elapsed seconds, actual commands, throughput and per-round median/p95.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import math
import platform
from pathlib import Path
import statistics
import socket
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tests'))
from cli_smoke import Client
from mcp_smoke import Fixture


def summarize(samples, commands_per_round):
    ordered = sorted(samples)
    elapsed = sum(samples)
    commands = len(samples) * commands_per_round
    return {
        'rounds': len(samples), 'commands_per_round': commands_per_round,
        'commands': commands, 'elapsed_seconds': elapsed,
        'commands_per_second': commands / elapsed,
        'round_median_ms': statistics.median(samples) * 1000,
        'round_p95_ms': ordered[max(0, math.ceil(len(ordered) * .95) - 1)] * 1000,
        'amortized_command_median_us': statistics.median(samples) * 1_000_000 / commands_per_round,
    }


def direct_pipeline(client, wire, command_count, writer):
    """Drain replies while a reused worker sends the batch, including large bodies."""
    sending = writer.submit(client.socket.sendall, wire)
    try:
        replies = [client.read() for _ in range(command_count)]
        sending.result()
        return replies
    except BaseException:
        # Interrupt the other direction before joining it; never strand a worker
        # behind a read failure or abandon an in-flight write for the next round.
        try:
            client.socket.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            sending.result()
        except Exception:
            pass
        raise


def measure(fixture, rounds, warmup, batch_sizes, value_bytes):
    session = fixture.mcp.open(protocol=2)
    direct = Client(fixture.server.address)
    direct.socket.settimeout(30)
    writer = ThreadPoolExecutor(max_workers=1, thread_name_prefix="ffkv-benchmark-writer")
    records = []
    value = 'v' * value_bytes
    try:
        for workload in ('ping', 'set_get'):
            for batch_size in batch_sizes:
                if workload == 'ping':
                    commands = [['PING'] for _ in range(batch_size)]
                    expected = [b'PONG'] * batch_size
                else:
                    commands = []
                    expected = []
                    for index in range(batch_size):
                        commands.extend([['SET', f'mcp:bench:{index}', value],
                                         ['GET', f'mcp:bench:{index}']])
                        expected.extend([b'OK', value.encode()])
                # Pre-encode direct frames just as a native client would retain
                # reusable request buffers. MCP still performs JSON serialization.
                wire = b''.join(Client.frame(command) for command in commands)
                timings = {'mcp_stdio': [], 'direct_resp': []}
                for round_index in range(warmup + rounds):
                    # Alternate order to reduce systematic warm-cache/order bias.
                    transports = ('mcp_stdio', 'direct_resp') if round_index % 2 == 0 else ('direct_resp', 'mcp_stdio')
                    for transport in transports:
                        start = time.perf_counter()
                        if transport == 'mcp_stdio':
                            replies = fixture.mcp.execute(commands, session=session)
                        else:
                            replies = direct_pipeline(direct, wire, len(commands), writer)
                        elapsed = time.perf_counter() - start
                        assert replies == expected, (transport, workload, replies[:3])
                        if round_index >= warmup:
                            timings[transport].append(elapsed)
                for transport, samples in timings.items():
                    records.append({'transport': transport, 'workload': workload,
                                    'batch_operations': batch_size,
                                    'mode': 'single_command' if len(commands) == 1 else 'pipeline',
                                    **summarize(samples, len(commands))})
    finally:
        direct.close()
        writer.shutdown(wait=True, cancel_futures=True)
        fixture.mcp.tool('ffkv_session', {'action': 'close', 'session': session})
    return records


def positive(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError('must be positive')
    return number


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', help='ffkv server binary')
    parser.add_argument('--mcp-binary', help='ffkv-mcp binary')
    parser.add_argument('--rounds', type=positive, default=100)
    parser.add_argument('--warmup', type=positive, default=10)
    parser.add_argument('--batch-sizes', type=positive, nargs='+', default=[1, 16, 128])
    parser.add_argument('--value-bytes', type=positive, default=64)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    if max(args.batch_sizes) * 2 > 1024:
        parser.error('set/get workload requires batch sizes <=512 (1024 command limit)')
    with Fixture(args.binary, args.mcp_binary) as fixture:
        # Benchmark transport rather than intentionally triggering result paging.
        fixture.mcp.close()
        fixture.mcps.remove(fixture.mcp)
        fixture.config['limits']['inline_result_bytes'] = 1024 * 1024
        fixture.config_path.write_text(json.dumps(fixture.config))
        fixture.mcp = fixture.new_mcp()
        results = measure(fixture, args.rounds, args.warmup, args.batch_sizes, args.value_bytes)
        report = {
            'server_binary': str(fixture.server_binary), 'mcp_binary': str(fixture.mcp_binary),
            'environment': {'platform': platform.platform(), 'machine': platform.machine(),
                            'python': platform.python_version()},
            'clock': 'time.perf_counter', 'warmup_rounds_excluded': args.warmup,
            'value_bytes': args.value_bytes,
            'direct_transport': 'persistent native RESP socket with a reused duplex writer thread, no CLI subprocess',
            'mcp_transport': 'persistent stdio process and explicit reusable RESP2 session',
            'measurement': 'send through complete ordered reply decoding; validation outside timed interval',
            'durability': 'strict, identical isolated server for both transports',
            'results': results,
        }
    text = json.dumps(report, indent=2) + '\n'
    if args.output:
        args.output.write_text(text)
    print(text, end='')


if __name__ == '__main__':
    main()
