#!/usr/bin/env python3
"""Exercise real Lua execution, atomicity, sandbox bounds and strict crash recovery.

Run after building ffkv: python3 tests/scripting_smoke.py --binary target/debug/ffkv
Optional --reference HOST:PORT evaluates only portable cases on owned UUID keys;
it never flushes the reference's script cache. SIGKILL tests process-crash recovery,
not power-loss durability. All subprocesses, sockets and worker threads are owned.
"""
import argparse
import hashlib
import json
import os
import select
import signal
import tempfile
import threading
import time
import uuid
from pathlib import Path

from cli_smoke import Client
from durability_smoke import Server
from queue_smoke import begin_wait, expect_quiet


def check(client, args, expected):
    actual = client.command(*args)
    assert actual == expected, (args, actual, expected)
    return actual


def error(client, args, code=b"ERR", contains=None):
    result = client.command(*args)
    assert isinstance(result, tuple) and result[0] == "error", (args, result)
    assert result[1].split(b" ", 1)[0] == code, (args, result, code)
    if contains is not None:
        assert contains.lower() in result[1].lower(), (args, result, contains)
    return result[1]


def portable(client, prefix):
    keys = [prefix + b":key:" + str(i).encode() for i in range(64)]
    keys[7] += b"\x00\xff\r\n"
    payload = b"\x00\xff\r\n$5\r\nhello"
    source = b"""
        local values = {}
        for i, key in ipairs(KEYS) do
            redis.call('SET', key, ARGV[1] .. i)
            values[i] = redis.call('GET', key)
        end
        return values
    """
    try:
        check(client, ["EVAL", source, len(keys), *keys, payload],
              [payload + str(i + 1).encode() for i in range(len(keys))])
        check(client, ["MGET", *keys],
              [payload + str(i + 1).encode() for i in range(len(keys))])
        check(client, ["EVAL", "return {true,false,3.9,-3.9,{1,2},'binary'..string.char(0,255)}", 0],
              [1, None, 3, -3, [1, 2], b"binary\x00\xff"])
        check(client, ["EVAL", "return {1,nil,3}", 0], [1])
        check(client, ["EVAL", "return redis.status_reply('READY')", 0], b"READY")
        error(client, ["EVAL", "return redis.error_reply('ERR deliberate')", 0], contains=b"deliberate")
        check(client, ["EVAL", "return redis.sha1hex(ARGV[1])", 0, payload], hashlib.sha1(payload).hexdigest().encode())
        check(client, ["EVAL", "return redis.call('GET',KEYS[1]) == false", 1, prefix + b":absent"], 1)
        check(client, ["EVAL", "return redis.replicate_commands()", 0], 1)
        check(client, ["EVAL", "local v=cjson.decode(ARGV[1]); return {v.n==cjson.null,v.a[2]==cjson.null,v.a[1],v.text}",
                       0, b'{"n":null,"a":[7,null],"text":"hello \\u2603"}'],
              [1, 1, 7, "hello \u2603".encode()])
        encoded = client.command("EVAL", "return cjson.encode({n=cjson.null,a={true,false,4},s=ARGV[1]})", 0, "\u2603")
        assert json.loads(encoded) == {"n": None, "a": [True, False, 4], "s": "\u2603"}, encoded
        check(client, ["EVAL", "return {bit.band(255,15,7),bit.bor(1,4),bit.bxor(255,15),bit.bnot(0),bit.lshift(1,33),bit.rshift(-1,31),bit.arshift(-1,31),bit.rol(1,31),bit.ror(1,1),bit.bswap(0x12345678),bit.tohex(-1),bit.tohex(255,-4),bit.tobit(4294967297)}", 0],
              [7, 5, 240, -1, 2, 1, -1, -2147483648, -2147483648, 2018915346, b"ffffffff", b"00FF", 1])
        # These are actual engine responses, not echo callbacks.
        h, s, z = (prefix + suffix for suffix in [b":hash", b":set", b":zset"])
        check(client, ["EVAL", "redis.call('HSET',KEYS[1],'field','value'); redis.call('SADD',KEYS[2],'member'); redis.call('ZADD',KEYS[3],1.25,'member'); return {redis.call('HGETALL',KEYS[1]),redis.call('SMEMBERS',KEYS[2]),redis.call('ZSCORE',KEYS[3],'member'),redis.call('ZRANGE',KEYS[3],0,-1,'WITHSCORES')}", 3, h, s, z],
              [[b"field", b"value"], [b"member"], b"1.25", [b"member", b"1.25"]])
        check(client, ["EVAL", "redis.setresp(3); local h=redis.call('HGETALL',KEYS[1]); local s=redis.call('SMEMBERS',KEYS[2]); local z=redis.call('ZSCORE',KEYS[3],'member'); return {h.map.field,s.set.member,z.double,redis.call('GET',KEYS[4])==nil}", 4, h, s, z, prefix + b":absent"],
              [b"value", 1, 1, 1])
        # A protected WRONGTYPE is a Lua error table; subsequent commands run.
        check(client, ["EVAL", "local e=redis.pcall('LPUSH',KEYS[1],'bad'); redis.call('SET',KEYS[2],'after'); return {string.sub(e.err,1,9),redis.call('GET',KEYS[2])}", 2, keys[0], keys[1]],
              [b"WRONGTYPE", b"after"])
        check(client, ["EVAL", "local count=0; for i=1,200 do local ok=pcall(function() redis.call('LPUSH',KEYS[1],'bad') end); if not ok then count=count+1 end end return count", 1, keys[0]], 200)
        error(client, ["EVAL", "redis.call('SET',KEYS[1],'written-before-error'); redis.call('LPUSH',KEYS[1],'bad'); return 99", 1, keys[0]], b"WRONGTYPE")
        check(client, ["GET", keys[0]], b"written-before-error")
        source, destination = keys[-2:]
        check(client, ["DEL", source, destination], 2)
        check(client, ["EVAL", "redis.call('RPUSH',KEYS[1],ARGV[1],'tail'); local moved=redis.call('LMOVE',KEYS[1],KEYS[2],'RIGHT','LEFT'); return {moved,redis.call('LRANGE',KEYS[1],0,-1),redis.call('LRANGE',KEYS[2],0,-1)}", 2, source, destination, payload],
              [b"tail", [payload], [b"tail"]])
        check(client, ["EVAL", "return redis.call('LMOVE',KEYS[1],KEYS[2],'LEFT','RIGHT')", 2, source, destination], payload)
        check(client, ["LRANGE", destination, 0, -1], [b"tail", payload])
    finally:
        client.command("DEL", *keys, prefix + b":hash", prefix + b":set", prefix + b":zset")


def cache_and_validation(client):
    check(client, ["SCRIPT", "FLUSH", "SYNC"], b"OK")
    source = b"return {KEYS[1],ARGV[1]}"
    sha = hashlib.sha1(source).hexdigest().encode()
    check(client, ["SCRIPT", "EXISTS", sha], [0])
    error(client, ["EVALSHA", sha, 0], b"NOSCRIPT")
    check(client, ["SCRIPT", "LOAD", source], sha)
    check(client, ["SCRIPT", "EXISTS", sha, b"0" * 40], [1, 0])
    check(client, ["EVALSHA", sha, 1, b"key\x00\xff", b"value\x00\xff"], [b"key\x00\xff", b"value\x00\xff"])
    invalid = b"local = this is not Lua"
    error(client, ["SCRIPT", "LOAD", invalid])
    check(client, ["SCRIPT", "EXISTS", hashlib.sha1(invalid).hexdigest()], [0])
    error(client, ["SCRIPT", "LOAD", b"\x1bLua\x51\x00"])
    error(client, ["EVAL", invalid, 0])
    for args in [["EVAL", "return 1"], ["EVAL", "return 1", "bad"],
                 ["EVAL", "return 1", -1], ["EVAL", "return 1", 2, "one"],
                 ["SCRIPT", "LOAD"], ["SCRIPT", "FLUSH", "nonsense"]]:
        error(client, args)
    check(client, ["SCRIPT", "FLUSH", "ASYNC"], b"OK")
    error(client, ["EVALSHA", sha, 1, "key", "value"], b"NOSCRIPT")
    check(client, ["EVAL", source, 1, "key", "value"], [b"key", b"value"])
    check(client, ["EVALSHA", sha, 1, "again", "loaded"], [b"again", b"loaded"])
    # Entry and owned-source byte limits are independently observable evictions.
    digests = [client.command("SCRIPT", "LOAD", f"return {i} -- cache-entry") for i in range(257)]
    check(client, ["SCRIPT", "EXISTS", digests[0], digests[-1]], [0, 1])
    bodies = [(f"return {i} -- ".encode() + b"x" * (6 * 1024 * 1024)) for i in range(3)]
    large = [client.command("SCRIPT", "LOAD", body) for body in bodies]
    assert all(isinstance(sha, bytes) and len(sha) == 40 for sha in large), large
    check(client, ["SCRIPT", "EXISTS", large[0], large[-1]], [0, 1])
    check(client, ["SCRIPT", "FLUSH"], b"OK")
    print("PASS compile validation, SHA cache lifecycle and both bounded-cache evictions", flush=True)


def sandbox(client, server):
    check(client, ["EVAL", "return {os==nil,io==nil,package==nil,debug==nil,require==nil,dofile==nil,loadfile==nil,loadstring==nil,load==nil,getfenv==nil,setfenv==nil,getmetatable==nil,newproxy==nil,coroutine==nil,string.dump==nil,('x').dump==nil}", 0], [1] * 16)
    check(client, ["EVAL", "leaked=42; math.abs=nil; table.insert=nil; string.sub=nil; cjson.encode=nil; bit.band=nil; redis.call=nil; return 1", 0], 1)
    check(client, ["EVAL", "local t={}; table.insert(t,'ok'); return {leaked==nil,math.abs(-2),string.sub('abc',2),cjson.encode({a=1}),bit.band(7,3),redis.call('PING'),t[1]}", 0],
          [1, 2, b"bc", b'{"a":1}', 3, b"PONG", b"ok"])
    error(client, ["EVAL", "redis.set_repl(0)", 0])
    for source in ["while true do end",
                   "pcall(function() while true do end end); return 'caught'",
                   "while true do pcall(function() while true do end end) end",
                   "return string.rep('x',64*1024*1024)",
                   "local function f(n) if n==0 then return 0 end return 1+f(n-1) end return f(1000)",
                   "local t={}; local v=t; for i=1,70 do v[1]={}; v=v[1] end return t"]:
        started = time.monotonic()
        error(client, ["EVAL", source, 0])
        assert time.monotonic() - started < 5, "sandbox failed to terminate promptly"
        check(client, ["PING"], b"PONG")
        check(client, ["EVAL", "return 17", 0], 17)
    # Exercise the real stderr sink, not a fake redis.log binding.
    marker = ("ffkv-lua-log-" + uuid.uuid4().hex).encode()
    check(client, ["EVAL", "redis.log(redis.LOG_NOTICE,ARGV[1]..'\\r\\nFORGED'..string.char(27)..'[31m'..string.char(0)); return 1", 0, marker], 1)
    ready, _, _ = select.select([server.process.stderr], [], [], 3)
    assert ready, "redis.log did not write to stderr"
    logged = os.read(server.process.stderr.fileno(), 16384)
    assert marker in logged and b"\nFORGED" not in logged and b"\x1b" not in logged and b"\r" not in logged and b"\x00" not in logged, logged
    print("PASS isolated libraries/globals, absent escape routes, sanitized actual logs and finite resource bounds", flush=True)


def list_move_wakeups(server):
    control, mover, consumer = [server.connect() for _ in range(3)]
    source, primed, final = [b"ffkv:test:scripting:move:" + suffix
                             for suffix in (b"source", b"primed", b"final")]
    begin_wait(mover, ["BLMOVE", primed, final, "RIGHT", "LEFT", 0], control)
    begin_wait(consumer, ["BRPOP", final, 0], control)
    check(control, ["EVAL", "redis.call('RPUSH',KEYS[1],'job'); redis.call('LMOVE',KEYS[1],KEYS[2],'RIGHT','LEFT'); return redis.call('LMOVE',KEYS[2],KEYS[1],'LEFT','RIGHT')", 2, source, primed], b"job")
    expect_quiet(mover)
    expect_quiet(consumer)
    check(control, ["EVAL", "return redis.call('LMOVE',KEYS[1],KEYS[2],'RIGHT','LEFT')", 2, source, primed], b"job")
    assert mover.read() == b"job"
    assert consumer.read() == [final, b"job"]
    check(control, ["EXISTS", source, primed, final], 0)
    print("PASS Lua LMOVE exposes only final state and wakes transitive BLMOVE consumers after atomic commit", flush=True)


def atomic_visibility(server):
    client = server.connect()
    observer = server.connect()
    keys = [f"ffkv:test:scripting:atomic:{i}" for i in range(96)]
    source = "for i,key in ipairs(KEYS) do redis.call('SET',key,ARGV[1]) end return #KEYS"
    check(client, ["EVAL", source, len(keys), *keys, 0], len(keys))
    ready = threading.Event()
    stop = threading.Event()
    failures = []
    snapshots = []

    def observe():
        try:
            for _ in range(2000):
                values = observer.command("MGET", *keys)
                assert len(values) == len(keys) and len(set(values)) == 1, values
                snapshots.append(values[0])
                ready.set()
                if stop.is_set():
                    return
            raise AssertionError("observer exhausted its finite request bound")
        except BaseException as exc:
            failures.append(exc)
            ready.set()

    thread = threading.Thread(target=observe, name="scripting-observer", daemon=True)
    thread.start()
    try:
        assert ready.wait(5), "observer did not start"
        for generation in range(1, 41):
            check(client, ["EVAL", source, len(keys), *keys, generation], len(keys))
    finally:
        stop.set()
        thread.join(10)
        if thread.is_alive():
            observer.socket.shutdown(2)
            thread.join(5)
    assert not thread.is_alive(), "observer worker did not terminate"
    if failures:
        raise failures[0]
    assert snapshots, "observer never read state"
    check(client, ["MGET", *keys], [b"40"] * len(keys))
    client.command("DEL", *keys)
    print("PASS concurrent MGET never sees a partial 96-key script across four shards", flush=True)


def recovery(binary):
    with tempfile.TemporaryDirectory(prefix="ffkv-scripting-") as directory:
        with Server(binary, Path(directory), durability="strict", extra_args=("--shards", "4")) as server:
            client = server.connect()
            keys = [b"ffkv:test:scripting:recover:" + str(i).encode() for i in range(6)]
            source = """
                redis.call('SET',KEYS[1],ARGV[1]);
                redis.call('HSET',KEYS[2],'field',ARGV[1]);
                redis.call('SADD',KEYS[3],'a','b','c');
                local popped=redis.call('SPOP',KEYS[3]);
                redis.call('ZADD',KEYS[4],1,'first',2,'second');
                local z=redis.call('ZPOPMIN',KEYS[4]);
                redis.call('RPUSH',KEYS[5],'left','right');
                local l=redis.call('LPOP',KEYS[5]);
                return {popped,z,l}
            """
            payload = b"saved\x00\xff\r\n"
            selected = client.command("EVAL", source, len(keys), *keys, payload)
            assert isinstance(selected, list) and len(selected) == 3, selected
            popped, zpop, lpop = selected
            assert popped in [b"a", b"b", b"c"] and zpop == [b"first", b"1"] and lpop == b"left", selected
            # No read intervenes between the acknowledged effects and SIGKILL.
            server.stop(signal.SIGKILL)
            server.extra_args = ("--shards", "1")
            server.start()
            client = server.connect()
            check(client, ["GET", keys[0]], payload)
            check(client, ["HGET", keys[1], "field"], payload)
            assert set(client.command("SMEMBERS", keys[2])) == {b"a", b"b", b"c"} - {popped}
            check(client, ["ZRANGE", keys[3], 0, -1, "WITHSCORES"], [b"second", b"2"])
            check(client, ["LRANGE", keys[4], 0, -1], [b"right"])
            error(client, ["EVALSHA", hashlib.sha1(source.encode()).hexdigest(), 0], b"NOSCRIPT")
            error(client, ["EVAL", "redis.call('SET',KEYS[1],'survives-error'); redis.call('LPUSH',KEYS[1],'wrongtype')", 1, keys[5]], b"WRONGTYPE")
            server.stop(signal.SIGKILL)
            server.extra_args = ("--shards", "4")
            server.start()
            client = server.connect()
            check(client, ["GET", keys[5]], b"survives-error")
            check(client, ["EVAL", "return redis.call('GET',KEYS[1])", 1, keys[5]], b"survives-error")
    print("PASS strict SIGKILL/reopen retains resolved random pops, all script effects and writes preceding Lua errors", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default="target/debug/ffkv")
    parser.add_argument("--reference", metavar="HOST:PORT")
    options = parser.parse_args()
    binary = str(Path(options.binary).resolve())
    with Server(binary, extra_args=("--shards", "4")) as server:
        client = server.connect()
        portable(client, b"ffkv:test:scripting:" + uuid.uuid4().hex.encode())
        print("PASS real binary-safe Lua calls, pcall, return shapes, cjson/null, bit operations and RESP3 call conversion", flush=True)
        cache_and_validation(client)
        sandbox(client, server)
        atomic_visibility(server)
        list_move_wakeups(server)
    recovery(binary)
    if options.reference:
        host, port = options.reference.rsplit(":", 1)
        reference = Client((host, int(port)))
        try:
            portable(reference, b"ffkv:test:scripting:" + uuid.uuid4().hex.encode())
        finally:
            reference.close()
        print("PASS portable scripting assertions against reference Redis", flush=True)


if __name__ == "__main__":
    main()
