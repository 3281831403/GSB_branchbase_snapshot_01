package proxy

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/branchbase/branchbase/internal/config"
)

// startTLSBackend starts a mock PostgreSQL backend that records every startup
// packet it receives and keeps connections open until the peer disconnects.
func startTLSBackend(t *testing.T) (addr string, received <-chan []byte) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start mock backend: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	packets := make(chan []byte, 16)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer func() { _ = c.Close() }()
				var lenBuf [4]byte
				if _, err := readFull(c, lenBuf[:]); err != nil {
					return
				}
				pktLen := binary.BigEndian.Uint32(lenBuf[:])
				if pktLen < 4 || pktLen > 1<<20 {
					return
				}
				payload := make([]byte, pktLen-4)
				if _, err := readFull(c, payload); err != nil {
					return
				}
				packets <- append(lenBuf[:], payload...)
				// Keep the connection open until the proxy hangs up.
				_, _ = readFull(c, make([]byte, 1))
			}(conn)
		}
	}()
	return listener.Addr().String(), packets
}

func readFull(conn net.Conn, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := conn.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func newTLSTestServer(t *testing.T, backendAddr string) (*Server, string) {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.Proxy.ListenPort = 0
	cfg.Proxy.TLS.Enabled = true
	cfg.Connection.Host = "127.0.0.1"
	cfg.Connection.Port = backendAddrPort(t, backendAddr)

	srv := NewServer(&cfg, t.TempDir(), nil)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("Server.Start failed: %v", err)
	}
	t.Cleanup(func() { _ = srv.Stop() })
	return srv, listenerAddr(srv)
}

func backendAddrPort(t *testing.T, addr string) int {
	t.Helper()
	_, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("bad backend addr %q: %v", addr, err)
	}
	var port int
	if _, err := fmtSscanf(portStr, &port); err != nil {
		t.Fatalf("bad backend port %q: %v", portStr, err)
	}
	return port
}

func fmtSscanf(s string, out *int) (int, error) {
	n := 0
	for _, ch := range s {
		if ch < '0' || ch > '9' {
			break
		}
		*out = *out*10 + int(ch-'0')
		n++
	}
	return n, nil
}

func writeSSLRequest(t *testing.T, conn net.Conn) {
	t.Helper()
	pkt := make([]byte, 8)
	binary.BigEndian.PutUint32(pkt[0:4], 8)
	binary.BigEndian.PutUint32(pkt[4:8], 80877103)
	if _, err := conn.Write(pkt); err != nil {
		t.Fatalf("failed to send SSLRequest: %v", err)
	}
	resp := make([]byte, 1)
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := readFull(conn, resp); err != nil {
		t.Fatalf("failed to read SSL response: %v", err)
	}
	if resp[0] != 'S' {
		t.Fatalf("expected proxy to accept TLS with 'S', got %q", resp[0])
	}
	_ = conn.SetReadDeadline(time.Time{})
}

func buildStartupPacket(database string) []byte {
	body := []byte("user\x00postgres\x00database\x00" + database + "\x00\x00")
	pkt := make([]byte, 8+len(body))
	binary.BigEndian.PutUint32(pkt[0:4], uint32(len(pkt)))
	binary.BigEndian.PutUint32(pkt[4:8], 196608) // ProtocolVersion3
	copy(pkt[8:], body)
	return pkt
}

func activeConnCount(s *Server) int {
	s.activeMu.Lock()
	defer s.activeMu.Unlock()
	return len(s.activeConns)
}

func waitForActiveConns(t *testing.T, srv *Server, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if activeConnCount(srv) == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("activeConns = %d, want %d (connection tracking leak)", activeConnCount(srv), want)
}

// TestTLSHandshakeFailureRecovery reproduces the production incident: after a
// burst of failed TLS handshakes (e.g. caused by network flakiness), the proxy
// must still accept and route brand-new connections without a restart.
func TestTLSHandshakeFailureRecovery(t *testing.T) {
	backendAddr, received := startTLSBackend(t)
	srv, addr := newTLSTestServer(t, backendAddr)

	// 1. Simulate a burst of failed handshakes: negotiate SSL, then send
	// garbage instead of a TLS ClientHello.
	const failures = 5
	for i := 0; i < failures; i++ {
		conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err != nil {
			t.Fatalf("failure %d: cannot even dial proxy: %v", i, err)
		}
		writeSSLRequest(t, conn)
		if _, err := conn.Write([]byte("definitely-not-a-tls-client-hello")); err != nil {
			t.Fatalf("failure %d: write garbage: %v", i, err)
		}
		// Wait for the proxy to give up on this connection.
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, _ = conn.Read(make([]byte, 1))
		_ = conn.Close()
	}

	// Failed handshakes must not leave stale connection tracking behind.
	waitForActiveConns(t, srv, 0)

	// 2. The network "recovers": a brand-new, well-behaved client must be able
	// to establish a fully routed connection without restarting the proxy.
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("proxy refuses new connections after handshake failures: %v", err)
	}
	defer func() { _ = conn.Close() }()
	writeSSLRequest(t, conn)

	tlsConn := tls.Client(conn, &tls.Config{InsecureSkipVerify: true})
	_ = tlsConn.SetDeadline(time.Now().Add(3 * time.Second))
	if err := tlsConn.Handshake(); err != nil {
		t.Fatalf("TLS handshake still failing after network recovery: %v", err)
	}
	if _, err := tlsConn.Write(buildStartupPacket("myapp_dev")); err != nil {
		t.Fatalf("failed to send startup packet over recovered connection: %v", err)
	}

	select {
	case pkt := <-received:
		if len(pkt) == 0 {
			t.Fatal("backend received empty startup packet after recovery")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("backend never received startup packet; proxy did not recover")
	}
}

// TestTLSConnectionTrackingDrains verifies that connections are removed from
// the server's active connection set once they terminate — including after
// the TLS upgrade — so connection state cannot accumulate and wedge the proxy.
func TestTLSConnectionTrackingDrains(t *testing.T) {
	backendAddr, _ := startTLSBackend(t)
	srv, addr := newTLSTestServer(t, backendAddr)

	const conns = 5
	for i := 0; i < conns; i++ {
		conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		writeSSLRequest(t, conn)
		tlsConn := tls.Client(conn, &tls.Config{InsecureSkipVerify: true})
		_ = tlsConn.SetDeadline(time.Now().Add(3 * time.Second))
		if err := tlsConn.Handshake(); err != nil {
			t.Fatalf("handshake %d: %v", i, err)
		}
		if _, err := tlsConn.Write(buildStartupPacket("myapp_dev")); err != nil {
			t.Fatalf("startup %d: %v", i, err)
		}
		_ = tlsConn.Close()
	}

	waitForActiveConns(t, srv, 0)
}
