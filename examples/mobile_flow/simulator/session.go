package simulator

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/mobiletoly/go-oversync/examples/internal/exampleauth"
)

// Session manages user authentication and example JWT tokens.
type Session struct {
	userID string
	auth   *exampleauth.TokenAuth
	logger *slog.Logger

	// Current session state
	token     string
	expiresAt time.Time
	isActive  bool

	mu sync.RWMutex
}

// NewSession creates a new session manager.
func NewSession(userID, jwtSecret string, logger *slog.Logger) *Session {
	return &Session{
		userID: userID,
		auth:   exampleauth.New(jwtSecret),
		logger: logger,
	}
}

// SignIn creates a new authenticated session.
func (s *Session) SignIn(userID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if verboseLog {
		s.logger.Info("Creating new session", "user_id", userID)
	}

	token, expiresAt, err := s.generateToken(userID)
	if err != nil {
		return fmt.Errorf("failed to generate token: %w", err)
	}

	s.userID = userID
	s.token = token
	s.expiresAt = expiresAt
	s.isActive = true

	if verboseLog {
		s.logger.Info("Session created successfully",
			"user_id", userID,
			"expires_at", expiresAt.Format(time.RFC3339))
	}

	return nil
}

// Detach clears the current authenticated app session state.
func (s *Session) Detach() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if verboseLog {
		s.logger.Info("Signing out", "user_id", s.userID)
	}

	s.token = ""
	s.expiresAt = time.Time{}
	s.isActive = false

	if verboseLog {
		s.logger.Info("Session cleared")
	}
}

// IsActive returns whether there's an active session
func (s *Session) IsActive() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.isActive {
		return false
	}

	if time.Now().After(s.expiresAt) {
		s.logger.Warn("Token expired", "expires_at", s.expiresAt.Format(time.RFC3339))
		return false
	}

	return true
}

// GetToken returns the current JWT token
func (s *Session) GetToken() (string, error) {
	s.mu.RLock()
	if !s.isActive {
		s.mu.RUnlock()
		return "", fmt.Errorf("no active session")
	}
	if time.Now().Add(5 * time.Minute).Before(s.expiresAt) {
		token := s.token
		s.mu.RUnlock()
		return token, nil
	}
	s.mu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.isActive {
		return "", fmt.Errorf("no active session")
	}
	if time.Now().Add(5 * time.Minute).After(s.expiresAt) {
		s.logger.Info("Refreshing token", "user_id", s.userID)

		token, expiresAt, err := s.generateToken(s.userID)
		if err != nil {
			return "", fmt.Errorf("failed to refresh token: %w", err)
		}

		s.token = token
		s.expiresAt = expiresAt

		s.logger.Info("Token refreshed", "expires_at", expiresAt.Format(time.RFC3339))
	}

	return s.token, nil
}

// GetUserID returns the current user ID
func (s *Session) GetUserID() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.userID
}

// CanRestore returns whether a session can be restored (simulates persistent storage)
func (s *Session) CanRestore() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.userID != ""
}

// Restore simulates restoring a session from persistent storage
func (s *Session) Restore() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.userID == "" {
		return fmt.Errorf("no session to restore")
	}

	if verboseLog {
		s.logger.Info("Restoring session", "user_id", s.userID)
	}

	token, expiresAt, err := s.generateToken(s.userID)
	if err != nil {
		return fmt.Errorf("failed to generate token during restore: %w", err)
	}

	s.token = token
	s.expiresAt = expiresAt
	s.isActive = true

	if verboseLog {
		s.logger.Info("Session restored successfully", "expires_at", expiresAt.Format(time.RFC3339))
	}

	return nil
}

// generateToken creates a new JWT token using server's auth logic.
func (s *Session) generateToken(userID string) (string, time.Time, error) {
	duration := 24 * time.Hour

	token, err := s.auth.GenerateToken(userID, duration)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("failed to generate token: %w", err)
	}

	expiresAt := time.Now().Add(duration)
	return token, expiresAt, nil
}
