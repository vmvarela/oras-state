// Package oras provides OCI registry operations for the Terraform state backend.
package orasstate

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	orasProperties "github.com/oras-project/oras-go/v3/registry/remote/properties"
)

// headerCapturingRoundTripper captures the request and returns a 200 response.
type headerCapturingRoundTripper struct {
	request *http.Request
}

func (h *headerCapturingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	h.request = req
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader("")),
	}, nil
}

// writeTempFile writes content to a temporary file and returns the path.
func writeTempFile(t *testing.T, content []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.pem")
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	return path
}

// generateCACert generates a self-signed CA certificate and returns it as PEM bytes.
func generateCACert(t *testing.T) []byte {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatalf("failed to generate serial number: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			CommonName: "Test CA",
		},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("failed to create certificate: %v", err)
	}

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
}

func TestUserAgentRoundTripper_DoesNotMutateOriginalRequest(t *testing.T) {
	// The RoundTrip method clones the request before modifying it, so the
	// original request passed in should be left untouched.
	capturer := &headerCapturingRoundTripper{}
	rt := &userAgentRoundTripper{
		next:      capturer,
		userAgent: "test-agent/1.0",
	}

	originalReq, err := http.NewRequest(http.MethodGet, "http://example.com/", nil)
	if err != nil {
		t.Fatal(err)
	}
	originalReq.Header.Set("User-Agent", "original/1.0")

	resp, err := rt.RoundTrip(originalReq)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })

	// Original request must be unchanged.
	if got := originalReq.Header.Get("User-Agent"); got != "original/1.0" {
		t.Errorf("original request User-Agent was modified: got %q, want %q", got, "original/1.0")
	}

	// The request that reached the inner round tripper must have the new
	// User-Agent set.
	if capturer.request == nil {
		t.Fatal("inner round tripper was never called")
	}
	if got := capturer.request.Header.Get("User-Agent"); got != "test-agent/1.0" {
		t.Errorf("inner request User-Agent = %q, want %q", got, "test-agent/1.0")
	}
}

func TestUserAgentRoundTripper_SetsUserAgent(t *testing.T) {
	// This implementation always sets the User-Agent header, overwriting any
	// existing value.
	capturer := &headerCapturingRoundTripper{}
	rt := &userAgentRoundTripper{
		next:      capturer,
		userAgent: "my-agent/2.0",
	}

	req, err := http.NewRequest(http.MethodGet, "http://example.com/", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("User-Agent", "existing-agent/1.0")

	resp, err := rt.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })

	if capturer.request == nil {
		t.Fatal("inner round tripper was never called")
	}
	if got := capturer.request.Header.Get("User-Agent"); got != "my-agent/2.0" {
		t.Errorf("User-Agent = %q, want %q", got, "my-agent/2.0")
	}
}

func TestBuildHTTPClient(t *testing.T) {
	t.Run("default client", func(t *testing.T) {
		client, err := BuildHTTPClient(false, "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if client == nil {
			t.Fatal("client is nil")
		}
		// No fixed overall timeout: operations carry their own context.
		if client.Timeout != 0 {
			t.Errorf("Timeout = %v, want 0 (context-driven deadlines only)", client.Timeout)
		}
	})

	t.Run("insecure client", func(t *testing.T) {
		client, err := BuildHTTPClient(true, "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if client == nil {
			t.Fatal("client is nil")
		}

		// Verify that the transport disables TLS verification
		transport, ok := client.Transport.(*http.Transport)
		if !ok {
			t.Fatalf("transport is %T, want *http.Transport", client.Transport)
		}
		if transport.TLSClientConfig == nil {
			t.Fatal("TLSClientConfig is nil, expected non-nil with InsecureSkipVerify=true")
		}
		if !transport.TLSClientConfig.InsecureSkipVerify {
			t.Errorf("InsecureSkipVerify = false, want true")
		}
	})

	t.Run("with valid CA file", func(t *testing.T) {
		caCert := generateCACert(t)
		caFile := writeTempFile(t, caCert)

		client, err := BuildHTTPClient(false, caFile)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if client == nil {
			t.Fatal("client is nil")
		}
	})

	t.Run("with non-existent CA file", func(t *testing.T) {
		_, err := BuildHTTPClient(false, "/nonexistent/ca.pem")
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "reading ca_file") {
			t.Errorf("error message does not contain %q: %v", "reading ca_file", err)
		}
	})

	t.Run("with invalid PEM in CA file", func(t *testing.T) {
		caFile := writeTempFile(t, []byte("not a valid PEM certificate"))

		_, err := BuildHTTPClient(false, caFile)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "no valid PEM certificates found") {
			t.Errorf("error message does not contain %q: %v", "no valid PEM certificates found", err)
		}
	})
}

func TestHTTPClientWithUserAgent(t *testing.T) {
	// Test the full chain: BuildHTTPClient + userAgentRoundTripper
	origVersion := Version
	Version = "1.0"
	t.Cleanup(func() { Version = origVersion })

	baseClient, err := BuildHTTPClient(false, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	client := &http.Client{
		Transport: &userAgentRoundTripper{
			next:      baseClient.Transport,
			userAgent: userAgent(),
		},
	}

	// Check that the transport is a userAgentRoundTripper.
	rt, ok := client.Transport.(*userAgentRoundTripper)
	if !ok {
		t.Fatalf("transport is %T, want *userAgentRoundTripper", client.Transport)
	}
	if rt.userAgent != "oras-state/1.0" {
		t.Errorf("userAgent = %q, want %q", rt.userAgent, "oras-state/1.0")
	}
}

// TestResolveCredentials_CallerFuncHostGuard pins the v3 credential adapter:
// the caller resolver answers for one registry host, so v3 upload-host
// challenges for any other host must stay anonymous.
func TestResolveCredentials_CallerFuncHostGuard(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name      string
		registry  string
		challenge string
		wantHost  string // host passed to the caller resolver; empty means never called
	}{
		{name: "configured registry", registry: "https://Registry.Example.com/", challenge: "registry.example.com", wantHost: "registry.example.com"},
		{name: "docker.io alias", registry: "docker.io", challenge: "docker.io", wantHost: "registry-1.docker.io"},
		{name: "other host", registry: "registry.example.com", challenge: "cdn.example.com"},
		{name: "other port", registry: "registry.example.com:5000", challenge: "registry.example.com:8443"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			cfg := Config{CredentialFunc: func(_ context.Context, hostport string) (Credential, error) {
				calls++
				if hostport != tt.wantHost {
					t.Errorf("resolver host = %q, want %q", hostport, tt.wantHost)
				}
				return Credential{Username: "user", Password: "pass"}, nil
			}}
			credFn := resolveCredentials(tt.registry, cfg)

			cred, err := credFn(ctx, orasProperties.Resource{Registry: tt.challenge})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.wantHost == "" {
				if calls != 0 {
					t.Fatalf("resolver calls = %d, want 0 for a foreign host", calls)
				}
				if cred.Username != "" || cred.Password != "" || cred.AccessToken != "" {
					t.Errorf("credential = %+v, want empty for a foreign host", cred)
				}
				return
			}
			if calls != 1 {
				t.Fatalf("resolver calls = %d, want 1", calls)
			}
			if cred.Username != "user" || cred.Password != "pass" {
				t.Errorf("credential = %+v, want the caller credential mapped", cred)
			}
		})
	}

	t.Run("resolver error", func(t *testing.T) {
		wantErr := errors.New("no credential")
		cfg := Config{CredentialFunc: func(context.Context, string) (Credential, error) {
			return Credential{}, wantErr
		}}
		credFn := resolveCredentials("registry.example.com", cfg)
		cred, err := credFn(ctx, orasProperties.Resource{Registry: "registry.example.com"})
		if !errors.Is(err, wantErr) {
			t.Fatalf("error = %v, want %v", err, wantErr)
		}
		if cred.Username != "" || cred.Password != "" || cred.AccessToken != "" {
			t.Errorf("credential = %+v, want empty on resolver error", cred)
		}
	})
}

// TestTokenCredential pins the credential shape each documented token path
// relies on: GHCR needs basic auth against its token endpoint, the rest take
// the token raw.
func TestTokenCredential(t *testing.T) {
	ghcr := tokenCredential("ghcr.io", "tok")
	if ghcr.Username != "x" || ghcr.Password != "tok" || ghcr.AccessToken != "" {
		t.Errorf("ghcr credential = %+v, want basic auth with username x", ghcr)
	}

	other := tokenCredential("registry.example.com", "tok")
	if other.AccessToken != "tok" || other.Username != "" || other.Password != "" {
		t.Errorf("credential = %+v, want the token as access token", other)
	}
}
