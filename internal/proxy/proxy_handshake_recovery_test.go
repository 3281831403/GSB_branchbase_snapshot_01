package proxy

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/branchbase/branchbase/internal/config"
)

// startEchoBackend starts a mock database backend that reads the forwarded
// startup packet and then echoes back everything it receives.
func startEchoBackend(t *testing.T) (addr string, startupReceived chan []byte) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start mock backend: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	startupReceived = make(chan []byte, 16)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer func() { _ = c.Close() }()
				buf := make([]byte, 4096)
				n, err := c.Read(buf)
				if n > 0 {
					startupReceived <- buf[:n]
				}
				if err != nil {
					return
				}
				_, _ = io.Copy(c, c) // echo
			}(conn)
		}
	}()
	return listener.Addr().String(), startupReceived
}

// startTLSProxy starts a TLS-enabled proxy in front of the mock backend.
func startTLSProxy(t *testing.T, backendAddr string) (srv *Server, addr string) {
	t.Helper()
	port := 0
	if _, p, err := net.SplitHostPort(backendAddr); err == nil {
		_, _ = fmt.Sscanf(p, "%d", &port)
	}

	cfg := config.DefaultConfig()
	cfg.Proxy.ListenPort = 0
	cfg.Proxy.TLS.Enabled = true
	cfg.Connection.Host = "127.0.0.1"
	cfg.Connection.Port = port

	srv = NewServer(&cfg, t.TempDir(), nil)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("Server.Start failed: %v", err)
	}
	t.Cleanup(func() { _ = srv.Stop() })

	addr = listenerAddr(srv)
	if addr == "" {
		t.Fatal("expected listen addr after Start")
	}
	return srv, addr
}

func sendSSLRequest(t *testing.T, conn net.Conn) {
	t.Helper()
	pkt := make([]byte, 8)
	binary.BigEndian.PutUint32(pkt[0:4], 8)
	binary.BigEndian.PutUint32(pkt[4:8], 80877103)
	if _, err := conn.Write(pkt); err != nil {
		t.Fatalf("failed to send SSLRequest: %v", err)
	}
	resp := make([]byte, 1)
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.ReadFull(conn, resp); err != nil {
		t.Fatalf("failed to read SSL response: %v", err)
	}
	if resp[0] != 'S' {
		t.Fatalf("expected proxy to accept TLS upgrade ('S'), got %q", resp[0])
	}
	_ = conn.SetReadDeadline(time.Time{})
}

func sendStartupPacket(t *testing.T, conn net.Conn) {
	t.Helper()
	body := []byte("user\x00postgres\x00database\x00myapp_dev\x00\x00")
	pkt := make([]byte, 8+len(body))
	binary.BigEndian.PutUint32(pkt[0:4], uint32(len(pkt)))
	binary.BigEndian.PutUint32(pkt[4:8], 196608) // protocol 3.0
	copy(pkt[8:], body)
	if _, err := conn.Write(pkt); err != nil {
		t.Fatalf("failed to send startup packet: %v", err)
	}
}

// openFullTLSConnection performs a complete successful handshake through the
// proxy: SSLRequest, TLS handshake, and a startup packet.
func openFullTLSConnection(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("failed to dial proxy: %v", err)
	}
	sendSSLRequest(t, conn)
	tlsConn := tls.Client(conn, &tls.Config{InsecureSkipVerify: true})
	_ = tlsConn.SetDeadline(time.Now().Add(3 * time.Second))
	if err := tlsConn.Handshake(); err != nil {
		t.Fatalf("TLS handshake with proxy failed: %v", err)
	}
	sendStartupPacket(t, tlsConn)
	_ = tlsConn.SetDeadline(time.Time{})
	return tlsConn
}

func activeConnCount(s *Server) int {
	s.activeMu.Lock()
	defer s.activeMu.Unlock()
	return len(s.activeConns)
}

// TestProxyRecoversAfterFailedTLSHandshakes reproduces the field report:
// after several consecutive failed TLS handshakes, the proxy must still be
// able to establish brand-new connections without a restart.
func TestProxyRecoversAfterFailedTLSHandshakes(t *testing.T) {
	backendAddr, startupReceived := startEchoBackend(t)
	_, addr := startTLSProxy(t, backendAddr)

	// 1. Simulate a network blip: several consecutive TLS handshake failures.
	for i := 0; i < 5; i++ {
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err != nil {
			t.Fatalf("failed handshake %d: dial: %v", i, err)
		}
		sendSSLRequest(t, conn)
		if _, err := conn.Write([]byte("corrupted-tls-record")); err != nil {
			t.Fatalf("failed handshake %d: write: %v", i, err)
		}
		_ = conn.Close()
	}
	time.Sleep(100 * time.Millisecond)

	// 2. The network recovers: a new connection must succeed end to end.
	tlsConn := openFullTLSConnection(t, addr)
	defer func() { _ = tlsConn.Close() }()

	select {
	case pkt := <-startupReceived:
		if len(pkt) == 0 {
			t.Fatal("backend received empty startup packet after recovery")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("proxy did not establish a new connection after failed handshakes (restart would be required)")
	}

	// Data must flow through the recovered connection.
	if _, err := tlsConn.Write([]byte("ping")); err != nil {
		t.Fatalf("write on recovered connection: %v", err)
	}
	buf := make([]byte, 4)
	_ = tlsConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.ReadFull(tlsConn, buf); err != nil {
		t.Fatalf("read on recovered connection: %v", err)
	}
	if string(buf) != "ping" {
		t.Fatalf("unexpected echo on recovered connection: %q", buf)
	}
}

// TestProxyStalledHandshakeReleasesResources verifies that a client stalling
// mid-handshake cannot pin the connection (and its backend connection)
// forever: the handshake deadline aborts it and the proxy keeps accepting
// new connections.
func TestProxyStalledHandshakeReleasesResources(t *testing.T) {
	backendAddr, startupReceived := startEchoBackend(t)
	srv, addr := startTLSProxy(t, backendAddr)
	srv.SetHandshakeTimeout(200 * time.Millisecond)

	// Stall several handshakes: negotiate the TLS upgrade, then go silent.
	stalled := make([]net.Conn, 0, 3)
	for i := 0; i < 3; i++ {
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err != nil {
			t.Fatalf("stalled dial %d: %v", i, err)
		}
		sendSSLRequest(t, conn)
		stalled = append(stalled, conn)
	}

	// The proxy must abort the stalled handshakes and release their resources.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if activeConnCount(srv) == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if n := activeConnCount(srv); n != 0 {
		t.Fatalf("stalled handshakes were not reaped: %d connection(s) still tracked", n)
	}
	for _, c := range stalled {
		_ = c.Close()
	}

	// The proxy must still establish new connections without a restart.
	tlsConn := openFullTLSConnection(t, addr)
	defer func() { _ = tlsConn.Close() }()
	select {
	case <-startupReceived:
	case <-time.After(2 * time.Second):
		t.Fatal("proxy failed to establish a new connection after stalled handshakes")
	}
}

// TestProxyTLSConnectionsReleasedOnClose ensures connection lifecycle
// tracking does not leak: once a TLS session ends, both the raw and the
// TLS-wrapped connection must leave the active set.
func TestProxyTLSConnectionsReleasedOnClose(t *testing.T) {
	backendAddr, _ := startEchoBackend(t)
	srv, addr := startTLSProxy(t, backendAddr)

	for i := 0; i < 3; i++ {
		conn := openFullTLSConnection(t, addr)
		if err := conn.Close(); err != nil {
			t.Fatalf("close %d: %v", i, err)
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if activeConnCount(srv) == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("active connection set leaked: %d entr(ies) remain after all connections closed", activeConnCount(srv))
}
