package sshmanager

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/ssh"

	"telssh/config"
)

// Session holds an active SSH connection and provides command execution.
type Session struct {
	client *ssh.Client
	vps    config.VPS
	mu     sync.Mutex
	quit   chan struct{} // closed on cleanup to stop keepalive
}

// Manager manages SSH sessions for multiple VPS servers per user.
type Manager struct {
	mu       sync.RWMutex
	sessions map[int64]*Session
	active   map[int64]string

	liveMu   sync.Mutex
	liveCxls map[int64]context.CancelFunc // one live session per user
}

// NewManager creates a new SSH manager.
func NewManager() *Manager {
	return &Manager{
		sessions: make(map[int64]*Session),
		active:   make(map[int64]string),
		liveCxls: make(map[int64]context.CancelFunc),
	}
}

func sshConnect(v config.VPS) (*ssh.Client, error) {
	var authMethods []ssh.AuthMethod

	if v.KeyPath != "" {
		key, err := os.ReadFile(v.KeyPath)
		if err == nil {
			signer, err := ssh.ParsePrivateKey(key)
			if err == nil {
				authMethods = append(authMethods, ssh.PublicKeys(signer))
			}
		}
	}

	if v.Password != "" {
		authMethods = append(authMethods, ssh.Password(v.Password))
	}

	if len(authMethods) == 0 {
		return nil, fmt.Errorf("no auth method for %s (set password or key_path)", v.Name)
	}

	cfg := &ssh.ClientConfig{
		User:            v.User,
		Auth:            authMethods,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         10 * time.Second,
	}

	addr := net.JoinHostPort(v.Host, fmt.Sprintf("%d", v.Port))
	client, err := ssh.Dial("tcp", addr, cfg)
	if err != nil {
		return nil, fmt.Errorf("ssh dial %s: %w", addr, err)
	}
	return client, nil
}

// keepAlive sends periodic keepalive requests to prevent stale connections.
func (s *Session) keepAlive() {
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-s.quit:
			return
		case <-t.C:
			s.mu.Lock()
			if s.client == nil {
				s.mu.Unlock()
				return
			}
			_, _, err := s.client.SendRequest("keepalive@openssh.com", true, nil)
			s.mu.Unlock()
			if err != nil {
				log.Printf("SSH keepalive failed for %s: %v", s.vps.Name, err)
				return
			}
		}
	}
}

// newSession creates a new SSH session with a timeout to handle stale connections.
func (s *Session) newSession() (*ssh.Session, error) {
	s.mu.Lock()
	if s.client == nil {
		s.mu.Unlock()
		return nil, fmt.Errorf("connection closed")
	}
	client := s.client
	s.mu.Unlock()

	type result struct {
		session *ssh.Session
		err     error
	}
	ch := make(chan result, 1)
	go func() {
		sess, err := client.NewSession()
		ch <- result{sess, err}
	}()

	select {
	case r := <-ch:
		if r.err != nil {
			return nil, fmt.Errorf("new session: %w", r.err)
		}
		return r.session, nil
	case <-time.After(15 * time.Second):
		return nil, fmt.Errorf("session creation timed out (connection may be stale, try /disconnect then /connect)")
	}
}

// Connect establishes an SSH session for a user to a specific VPS.
func (m *Manager) Connect(userID int64, v config.VPS) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if old, ok := m.sessions[userID]; ok {
		old.Close()
	}

	client, err := sshConnect(v)
	if err != nil {
		return err
	}

	sess := &Session{client: client, vps: v, quit: make(chan struct{})}
	go sess.keepAlive()

	m.sessions[userID] = sess
	m.active[userID] = v.Name
	return nil
}

// Disconnect closes the SSH session for a user.
func (m *Manager) Disconnect(userID int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if s, ok := m.sessions[userID]; ok {
		s.Close()
		delete(m.sessions, userID)
		delete(m.active, userID)
	}
}

// Exec runs a command on the user's active VPS and returns combined output.
func (m *Manager) Exec(userID int64, cmd string, maxOutput int) (string, error) {
	m.mu.RLock()
	s, ok := m.sessions[userID]
	m.mu.RUnlock()

	if !ok {
		return "", fmt.Errorf("not connected to any server, use /connect first")
	}

	return s.Exec(cmd, maxOutput)
}

// ActiveServer returns the name of the active server for a user.
func (m *Manager) ActiveServer(userID int64) string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.active[userID]
}

// StopLive cancels any running live session for a user.
func (m *Manager) StopLive(userID int64) {
	m.liveMu.Lock()
	defer m.liveMu.Unlock()
	if cancel, ok := m.liveCxls[userID]; ok {
		cancel()
		delete(m.liveCxls, userID)
	}
}

// HasLive returns true if a live session is running for a user.
func (m *Manager) HasLive(userID int64) bool {
	m.liveMu.Lock()
	defer m.liveMu.Unlock()
	_, ok := m.liveCxls[userID]
	return ok
}

// StreamCallback is called periodically with accumulated output.
// Return false to stop streaming.
type StreamCallback func(output string, final bool) bool

// ExecStream runs a command and calls cb with accumulated output every interval.
// It blocks until the command finishes, context is cancelled, or timeout expires.
func (m *Manager) ExecStream(ctx context.Context, userID int64, cmd string, maxOutput int, interval time.Duration, timeout time.Duration, cb StreamCallback) error {
	m.mu.RLock()
	s, ok := m.sessions[userID]
	m.mu.RUnlock()

	if !ok {
		return fmt.Errorf("not connected to any server, use /connect first")
	}

	// Register live session cancel
	liveCtx, cancel := context.WithCancel(ctx)
	m.liveMu.Lock()
	if prev, exists := m.liveCxls[userID]; exists {
		prev()
	}
	m.liveCxls[userID] = cancel
	m.liveMu.Unlock()

	defer func() {
		m.liveMu.Lock()
		delete(m.liveCxls, userID)
		m.liveMu.Unlock()
		cancel()
	}()

	return s.ExecStream(liveCtx, cmd, maxOutput, interval, timeout, cb)
}

// ExecStream runs a streaming command on the session.
func (s *Session) ExecStream(ctx context.Context, cmd string, maxOutput int, interval time.Duration, timeout time.Duration, cb StreamCallback) error {
	session, err := s.newSession()
	if err != nil {
		return err
	}
	defer session.Close()

	modes := ssh.TerminalModes{
		ssh.ECHO:          0,
		ssh.TTY_OP_ISPEED: 14400,
		ssh.TTY_OP_OSPEED: 14400,
	}
	if pErr := session.RequestPty("xterm", 80, 200, modes); pErr != nil {
		log.Printf("PTY request failed: %v (continuing without PTY)", pErr)
	}

	var buf bytes.Buffer
	var bufMu sync.Mutex
	lw := &LimitedWriter{W: &buf, N: maxOutput + 500}
	session.Stdout = &syncWriter{w: lw, mu: &bufMu}
	session.Stderr = &syncWriter{w: lw, mu: &bufMu}

	// Use Start + Wait so we surface start errors immediately
	if err := session.Start(cmd); err != nil {
		return fmt.Errorf("start command: %w", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- session.Wait()
	}()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	timeoutCh := time.After(timeout)
	lastOutput := ""
	firstTick := true

	getOutput := func() string {
		bufMu.Lock()
		raw := buf.String()
		bufMu.Unlock()
		output := strings.TrimSpace(raw)
		output = stripAnsi(output)
		if len(output) > maxOutput {
			output = output[:maxOutput] + "\n... (truncated)"
		}
		return output
	}

	for {
		select {
		case <-ctx.Done():
			// User pressed stop
			_ = session.Signal(ssh.SIGINT)
			time.Sleep(200 * time.Millisecond)
			_ = session.Signal(ssh.SIGKILL)
			output := getOutput()
			cb(output, true)
			return nil

		case <-timeoutCh:
			_ = session.Signal(ssh.SIGKILL)
			output := getOutput()
			cb(output+"\n\n⏰ Timed out", true)
			return nil

		case err := <-done:
			output := getOutput()
			if output == "" && err != nil {
				cb("Error: "+err.Error(), true)
			} else {
				cb(output, true)
			}
			return nil

		case <-ticker.C:
			output := getOutput()
			// Always fire callback on first tick (even if no output yet)
			if output != lastOutput || firstTick {
				firstTick = false
				lastOutput = output
				if !cb(output, false) {
					_ = session.Signal(ssh.SIGKILL)
					return nil
				}
			}
		}
	}
}

// syncWriter is a thread-safe writer wrapper.
type syncWriter struct {
	w  io.Writer
	mu *sync.Mutex
}

func (sw *syncWriter) Write(p []byte) (int, error) {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	return sw.w.Write(p)
}

// IsConnected checks if a user has an active SSH session.
func (m *Manager) IsConnected(userID int64) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.sessions[userID]
	return ok
}

// Close closes the session's SSH client and stops keepalive.
func (s *Session) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.client != nil {
		select {
		case <-s.quit:
			// already closed
		default:
			close(s.quit)
		}
		s.client.Close()
		s.client = nil
	}
}

// Exec runs a single command and returns the output.
func (s *Session) Exec(cmd string, maxOutput int) (string, error) {
	session, err := s.newSession()
	if err != nil {
		return "", err
	}
	defer session.Close()

	modes := ssh.TerminalModes{
		ssh.ECHO:          0,
		ssh.TTY_OP_ISPEED: 14400,
		ssh.TTY_OP_OSPEED: 14400,
	}
	if pErr := session.RequestPty("xterm", 80, 200, modes); pErr != nil {
		log.Printf("PTY request failed: %v (continuing without PTY)", pErr)
	}

	var buf bytes.Buffer
	lw := &LimitedWriter{W: &buf, N: maxOutput + 500}
	session.Stdout = lw
	session.Stderr = lw

	done := make(chan error, 1)
	go func() {
		done <- session.Run(cmd)
	}()

	select {
	case err := <-done:
		output := strings.TrimSpace(buf.String())
		output = stripAnsi(output)
		if len(output) > maxOutput {
			output = output[:maxOutput] + "\n... (truncated)"
		}
		if output == "" && err != nil {
			return "", fmt.Errorf("command failed: %w", err)
		}
		return output, nil
	case <-time.After(60 * time.Second):
		_ = session.Signal(ssh.SIGKILL)
		return buf.String(), fmt.Errorf("command timed out after 60s")
	}
}

// LimitedWriter stops writing after N bytes.
type LimitedWriter struct {
	W io.Writer
	N int
	n int
}

func (lw *LimitedWriter) Write(p []byte) (int, error) {
	if lw.n >= lw.N {
		return len(p), nil
	}
	remaining := lw.N - lw.n
	if len(p) > remaining {
		p = p[:remaining]
	}
	n, err := lw.W.Write(p)
	lw.n += n
	return n, err
}

func stripAnsi(s string) string {
	var result strings.Builder
	result.Grow(len(s))
	i := 0
	for i < len(s) {
		if s[i] == '\x1b' {
			i++
			if i < len(s) && s[i] == '[' {
				i++
				for i < len(s) && !((s[i] >= 'A' && s[i] <= 'Z') || (s[i] >= 'a' && s[i] <= 'z')) {
					i++
				}
				if i < len(s) {
					i++
				}
			}
			continue
		}
		if s[i] < 32 && s[i] != '\n' && s[i] != '\t' && s[i] != '\r' {
			i++
			continue
		}
		result.WriteByte(s[i])
		i++
	}
	return result.String()
}

// ExecCancel runs a command that can be cancelled via context.
// When cancelled, it sends SIGINT then SIGKILL to the remote process.
func (m *Manager) ExecCancel(ctx context.Context, userID int64, cmd string, maxOutput int) (string, error) {
	m.mu.RLock()
	s, ok := m.sessions[userID]
	m.mu.RUnlock()

	if !ok {
		return "", fmt.Errorf("not connected to any server, use /connect first")
	}

	return s.ExecCancel(ctx, cmd, maxOutput)
}

// ExecCancel runs a single command with cancellation support.
func (s *Session) ExecCancel(ctx context.Context, cmd string, maxOutput int) (string, error) {
	session, err := s.newSession()
	if err != nil {
		return "", err
	}
	defer session.Close()

	modes := ssh.TerminalModes{
		ssh.ECHO:          0,
		ssh.TTY_OP_ISPEED: 14400,
		ssh.TTY_OP_OSPEED: 14400,
	}
	if pErr := session.RequestPty("xterm", 80, 200, modes); pErr != nil {
		log.Printf("PTY request failed: %v (continuing without PTY)", pErr)
	}

	var buf bytes.Buffer
	lw := &LimitedWriter{W: &buf, N: maxOutput + 500}
	session.Stdout = lw
	session.Stderr = lw

	done := make(chan error, 1)
	go func() {
		done <- session.Run(cmd)
	}()

	select {
	case <-ctx.Done():
		_ = session.Signal(ssh.SIGINT)
		time.Sleep(200 * time.Millisecond)
		_ = session.Signal(ssh.SIGKILL)
		output := strings.TrimSpace(buf.String())
		output = stripAnsi(output)
		if len(output) > maxOutput {
			output = output[:maxOutput] + "\n... (truncated)"
		}
		if output == "" {
			return "⚠️ Cancelled (no output)", nil
		}
		return output + "\n\n⚠️ Cancelled", nil
	case err := <-done:
		output := strings.TrimSpace(buf.String())
		output = stripAnsi(output)
		if len(output) > maxOutput {
			output = output[:maxOutput] + "\n... (truncated)"
		}
		if output == "" && err != nil {
			return "", fmt.Errorf("command failed: %w", err)
		}
		return output, nil
	case <-time.After(120 * time.Second):
		_ = session.Signal(ssh.SIGKILL)
		return buf.String(), fmt.Errorf("command timed out after 120s")
	}
}

// ReadFile reads a remote file via cat.
func (m *Manager) ReadFile(userID int64, path string, maxBytes int) (string, error) {
	m.mu.RLock()
	s, ok := m.sessions[userID]
	m.mu.RUnlock()

	if !ok {
		return "", fmt.Errorf("not connected to any server, use /connect first")
	}

	return s.Exec(fmt.Sprintf("cat %s", shellQuote(path)), maxBytes)
}

// WriteFile writes content to a remote file.
func (m *Manager) WriteFile(userID int64, path, content string) error {
	m.mu.RLock()
	s, ok := m.sessions[userID]
	m.mu.RUnlock()

	if !ok {
		return fmt.Errorf("not connected to any server, use /connect first")
	}

	cmd := fmt.Sprintf("cat > %s << 'VPSADMIN_EOF'\n%s\nVPSADMIN_EOF", shellQuote(path), content)
	_, err := s.Exec(cmd, 1000)
	return err
}

// shellQuote wraps a path in single quotes for safe shell usage.
func shellQuote(s string) string {
	escaped := strings.ReplaceAll(s, "'", "'\\''")
	return "'" + escaped + "'"
}

// ExecStreamRun runs a streaming command without registering as a live session.
// Use this for normal /run commands that need real-time output.
func (m *Manager) ExecStreamRun(ctx context.Context, userID int64, cmd string, maxOutput int, interval, timeout time.Duration, cb StreamCallback) error {
	m.mu.RLock()
	s, ok := m.sessions[userID]
	m.mu.RUnlock()

	if !ok {
		return fmt.Errorf("not connected to any server, use /connect first")
	}

	return s.ExecStream(ctx, cmd, maxOutput, interval, timeout, cb)
}

// DownloadFile reads a file from the remote server as raw bytes.
func (m *Manager) DownloadFile(userID int64, path string, maxBytes int) ([]byte, string, error) {
	m.mu.RLock()
	s, ok := m.sessions[userID]
	m.mu.RUnlock()

	if !ok {
		return nil, "", fmt.Errorf("not connected to any server, use /connect first")
	}

	data, err := s.downloadFile(path, maxBytes)
	if err != nil {
		return nil, "", err
	}

	basename := path
	if idx := strings.LastIndex(path, "/"); idx >= 0 {
		basename = path[idx+1:]
	}
	return data, basename, nil
}

// UploadFile writes data to a file on the remote server.
func (m *Manager) UploadFile(userID int64, path string, data []byte) error {
	m.mu.RLock()
	s, ok := m.sessions[userID]
	m.mu.RUnlock()

	if !ok {
		return fmt.Errorf("not connected to any server, use /connect first")
	}

	return s.uploadFile(path, data)
}

// downloadFile reads a file from the remote server via cat.
func (s *Session) downloadFile(path string, maxBytes int) ([]byte, error) {
	session, err := s.newSession()
	if err != nil {
		return nil, err
	}
	defer session.Close()

	var buf bytes.Buffer
	lw := &LimitedWriter{W: &buf, N: maxBytes}
	session.Stdout = lw
	session.Stderr = io.Discard

	if err := session.Run(fmt.Sprintf("cat %s", shellQuote(path))); err != nil {
		if buf.Len() > 0 {
			return buf.Bytes(), nil
		}
		return nil, fmt.Errorf("download failed: %w", err)
	}
	return buf.Bytes(), nil
}

// uploadFile writes data to a file on the remote server via cat.
func (s *Session) uploadFile(path string, data []byte) error {
	session, err := s.newSession()
	if err != nil {
		return err
	}
	defer session.Close()

	session.Stdin = bytes.NewReader(data)
	if err := session.Run(fmt.Sprintf("cat > %s", shellQuote(path))); err != nil {
		return fmt.Errorf("upload failed: %w", err)
	}
	return nil
}

// StopAllLive cancels all running live sessions.
func (m *Manager) StopAllLive() {
	m.liveMu.Lock()
	defer m.liveMu.Unlock()
	for uid, cancel := range m.liveCxls {
		cancel()
		delete(m.liveCxls, uid)
	}
}

// CloseAll cleans up all sessions.
func (m *Manager) CloseAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, s := range m.sessions {
		s.Close()
	}
	m.sessions = make(map[int64]*Session)
	m.active = make(map[int64]string)
}
