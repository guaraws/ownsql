#!/usr/bin/env python3
"""Black-box protocol, COPY, crash/reopen and psql tests; no Python packages."""
import os, pathlib, socket, struct, subprocess, tempfile, time
ROOT = pathlib.Path(__file__).resolve().parents[2]
BINARY = pathlib.Path(os.environ.get('OWNSQL_SERVER', ROOT / 'target/debug/server'))

def send(s, kind, body):
    s.sendall(kind + struct.pack('!i', len(body) + 4) + body)

def exact(s, n):
    b = b''
    while len(b) < n:
        chunk = s.recv(n - len(b))
        if not chunk: raise EOFError()
        b += chunk
    return b

def packet(s):
    kind = exact(s, 1)
    n, = struct.unpack('!i', exact(s, 4))
    return kind, exact(s, n - 4)

def until_ready(s, state=b'I'):
    frames = []
    while True:
        frame = packet(s); frames.append(frame)
        if frame[0] == b'Z':
            assert frame[1] == state
            return frames

def connect(port):
    s = socket.create_connection(('127.0.0.1', port), timeout=20)
    s.sendall(struct.pack('!ii', 8, 80877103)); assert exact(s, 1) == b'N'
    body = struct.pack('!i', 196608) + b'user\0test\0database\0test\0\0'
    s.sendall(struct.pack('!i', len(body) + 4) + body)
    frames = until_ready(s); assert frames[0] == (b'R', b'\0\0\0\0')
    return s

def query(s, sql, expect_error=False, state=b'I'):
    send(s, b'Q', sql.encode() + b'\0')
    frames = until_ready(s, state)
    errors = [b for k, b in frames if k == b'E']
    assert bool(errors) == expect_error, (sql, frames)
    rows = []
    for k, body in frames:
        if k != b'D': continue
        n, = struct.unpack('!h', body[:2]); pos = 2; row = []
        for _ in range(n):
            length, = struct.unpack('!i', body[pos:pos+4]); pos += 4
            row.append(None if length == -1 else body[pos:pos+length].decode())
            if length >= 0: pos += length
        rows.append(row)
    return rows

def main():
    with tempfile.TemporaryDirectory(prefix='ownsql-smoke-') as directory:
        with socket.socket() as probe:
            probe.bind(('127.0.0.1', 0)); port = probe.getsockname()[1]
        def start():
            log = open(pathlib.Path(directory) / 'server.log', 'a')
            process = subprocess.Popen([str(BINARY), '--listen', f'127.0.0.1:{port}', '--data-dir', directory + '/db'], stdout=log, stderr=log)
            log.close()
            for _ in range(300):
                if process.poll() is not None: raise RuntimeError(pathlib.Path(directory, 'server.log').read_text())
                try: return process, connect(port)
                except (ConnectionRefusedError, OSError): time.sleep(.1)
            process.kill(); process.wait(); raise TimeoutError('server startup')
        p, s = start()
        try:
            assert query(s, 'SELECT 1') == [['1']]
            assert query(s, 'SELECT TRUE, FALSE, CAST(NULL AS BIGINT)') == [['t', 'f', None]]
            query(s, 'CREATE TABLE events (id BIGINT, amount BIGINT, label TEXT)')
            query(s, "INSERT INTO events VALUES (1,10,'a'),(2,20,NULL)")
            query(s, "INSERT INTO events VALUES (3,30,'ok'),(4,'bad','fail')", True)
            assert query(s, 'SELECT COUNT(*), SUM(amount) FROM events') == [['2', '30']]
            query(s, 'CREATE TABLE unsupported (id BIGINT REFERENCES missing(id))', True)
            query(s, 'BEGIN', state=b'T')
            query(s, 'ROLLBACK')
            assert query(s, 'SELECT 2') == [['2']]
            send(s, b'P', b'\0SELECT 1\0\0\0')
            assert packet(s)[0] == b'1'
            send(s, b'S', b''); until_ready(s)
            assert query(s, 'SELECT 3') == [['3']]
            send(s, b'Q', b'COPY events FROM STDIN\0'); assert packet(s)[0] == b'G'
            send(s, b'd', b'3\t30\thello\\tworld\n4\t40\t\\N\n'); send(s, b'c', b'')
            frames = until_ready(s); assert any(k == b'C' and b == b'COPY 2\0' for k, b in frames), frames
            assert query(s, 'SELECT id, label FROM events ORDER BY id')[-2:] == [['3', 'hello\tworld'], ['4', None]]
            # Ledger stores only successfully acknowledged writes, outside database files.
            pathlib.Path(directory, 'confirmed-ledger.txt').write_text('1\n2\n3\n4\n')
            p.kill(); p.wait(); s.close()
            p, s = start()
            assert query(s, 'SELECT COUNT(*), SUM(amount) FROM events') == [['4', '100']]
            env = dict(os.environ, PGSSLMODE='disable')
            result = subprocess.run(['psql', '-X', '-h', '127.0.0.1', '-p', str(port), '-U', 'test', '-d', 'test', '-At', '-v', 'ON_ERROR_STOP=1', '-c', 'SELECT SUM(amount) FROM events'], env=env, capture_output=True, text=True, check=True)
            assert result.stdout.strip() == '100', result
            # Bad length must close only this session, not crash the server.
            bad = connect(port); bad.sendall(b'Q' + struct.pack('!i', 3)); assert bad.recv(1) == b''; bad.close()
            assert query(s, 'SELECT 4') == [['4']]
            print('PASS: PostgreSQL simple protocol, psql, NULL/bool, errors, extended Parse/Sync, COPY, atomic failed INSERT, SIGKILL recovery, malformed frame isolation')
        finally:
            s.close()
            if p.poll() is None: p.terminate()
            p.wait(timeout=15)

if __name__ == '__main__': main()
