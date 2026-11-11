// Package orasstate provides Terraform/OpenTofu state storage on OCI registries using ORAS.
package orasstate

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	orasRemote "github.com/oras-project/oras-go/v3/registry/remote"
	orasAuth "github.com/oras-project/oras-go/v3/registry/remote/auth"
	orasCredentials "github.com/oras-project/oras-go/v3/registry/remote/credentials"
	orasProperties "github.com/oras-project/oras-go/v3/registry/remote/properties"
)

// BuildHTTPClient constructs an *http.Client with the specified TLS settings.
// When skipVerify is true, certificate verification is disabled.
// When caFile is non-empty, it is loaded as the trusted CA pool.
func BuildHTTPClient(skipVerify bool, caFile string) (*http.Client, error) {
	transport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		transport = &http.Transport{}
	}
	t := transport.Clone()

	if t.TLSClientConfig == nil {
		t.TLSClientConfig = &tls.Config{} //nolint:gosec
	}
	t.TLSClientConfig.InsecureSkipVerify = skipVerify // #nosec G402 -- InsecureSkipVerify is configurable by the caller; the zero-value Config verifies certificates

	if caFile != "" {
		pem, err := os.ReadFile(caFile) // #nosec G304 -- the path is supplied by the caller and the PEM content is used as the trusted CA pool
		if err != nil {
			return nil, fmt.Errorf("reading ca_file %q: %w", caFile, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("ca_file %q: no valid PEM certificates found", caFile)
		}
		t.TLSClientConfig.RootCAs = pool
	}

	// No overall client Timeout: individual operations carry their own
	// context (with deadlines set by the caller), and a fixed 30s cap breaks
	// long-running transfers such as large state pushes.
	return &http.Client{Transport: t}, nil
}

// Version is the library version, set at build time via -ldflags
// (e.g. -X 'github.com/vmvarela/oras-state.Version=1.0.0').
// Defaults to "dev" for development builds.
var Version = "dev"

// userAgent returns the User-Agent string for HTTP requests.
func userAgent(configured ...string) string {
	if len(configured) > 0 && configured[0] != "" {
		return configured[0]
	}
	return "oras-state/" + Version
}

// WorkspaceLayout controls how logical state/workspace names map to OCI tags.
type WorkspaceLayout uint8

const (
	// WorkspaceLayoutHashed is the default, collision-resistant layout used by
	// the strict hashed layout. Every workspace name maps to its full SHA-256.
	WorkspaceLayoutHashed WorkspaceLayout = iota

	// WorkspaceLayoutLegacy preserves the historical literal-tag layout
	// 1.13.0 and earlier: valid OCI tag names remain literal, while invalid
	// names use a short "ws-" hash fallback. Use this only for compatibility
	// with existing literal-tag repositories.
	WorkspaceLayoutLegacy
)

// Credential is a registry credential returned by a caller-supplied resolver.
type Credential struct {
	Username    string
	Password    string
	AccessToken string
}

// CredentialFunc resolves credentials for a registry host.
type CredentialFunc func(ctx context.Context, hostport string) (Credential, error)

// Config holds all configurable options for the ORAS client.
type Config struct {
	// WorkspaceLayout selects the OCI tag layout. The zero value is the
	// strict SHA-256 layout used by the strict hashed layout.
	WorkspaceLayout WorkspaceLayout

	// CredentialFunc optionally delegates credential resolution to the caller.
	// When set it takes precedence over built-in token/env/config discovery.
	CredentialFunc CredentialFunc
	// UserAgent overrides the default oras-state/<version> User-Agent.
	UserAgent string

	// PlainHTTP explicitly selects HTTP instead of HTTPS.
	PlainHTTP bool
	// TLSSkipVerify disables HTTPS certificate verification without selecting HTTP.
	TLSSkipVerify bool
	// CAFile is the path to a PEM file with custom CA certificates for TLS.
	CAFile string
	// Username, Password, Token: tests/library only, see resolveCredentials.
	// Username is the explicit registry username (priority 2 credential).
	Username string
	// Password is the explicit registry password (priority 2 credential).
	Password string
	// Token is the explicit registry access token (highest credential priority).
	Token string
	// Compression gzip-compresses state layers when true.
	Compression bool // gzip when true
	// LockTTL is the lock lease duration; 0 means locks never expire.
	LockTTL time.Duration
	// MaxVersions is the target number of retained state versions. A value
	// <= 0 stops publishing version history (already-published history can
	// remain readable). Retention is best-effort: legacy or ambiguous
	// entries may be preserved, and ghcr.io never prunes automatically —
	// history stays published and readable there, but MaxVersions does not
	// bound it.
	MaxVersions int
	// MaxStateSize is the upper bound on state data; <= 0 uses the 256 MiB default.
	MaxStateSize int64
	// HTTPClient is an optional pre-configured HTTP client; one is built from
	// TLSSkipVerify/CAFile when nil.
	HTTPClient *http.Client
}

// Client is the top-level ORAS client that holds the shared OCI repository
// client and configuration for state operations.
type Client struct {
	repoClient   *orasRepositoryClient
	config       Config
	retentionSem chan struct{}
	retentionWg  sync.WaitGroup
}

// orasRepository is the minimal interface for OCI repository operations
// required by the state backend.
type orasRepository interface {
	Push(ctx context.Context, expected ocispec.Descriptor, content io.Reader) error
	PushReference(ctx context.Context, expected ocispec.Descriptor, content io.Reader, reference string) error
	Fetch(ctx context.Context, target ocispec.Descriptor) (io.ReadCloser, error)
	Exists(ctx context.Context, target ocispec.Descriptor) (bool, error)
	Resolve(ctx context.Context, reference string) (ocispec.Descriptor, error)
	Tag(ctx context.Context, desc ocispec.Descriptor, reference string) error
	Delete(ctx context.Context, target ocispec.Descriptor) error
	Tags(ctx context.Context, last string, fn func(tags []string) error) error
}

// orasRepositoryClient wraps the ORAS repository client with additional
// configuration for the state backend.
type orasRepositoryClient struct {
	repository   string // full reference: registry/repository
	inner        orasRepository
	credentialFn orasCredentials.CredentialFunc
	httpClient   *http.Client
}

// accessToken returns the access token for GHCR API calls. Falls back to the
// resolved credential (AccessToken, then Password) so that configured
// credentials (CLI config, Docker config files, helpers) also work for the
// GHCR delete fallback, not just env/explicit tokens. The operation context
// is propagated into the caller CredentialFunc so cancellation (and any
// per-call values) reach credential resolution; discovery policy is unchanged.
func (r *orasRepositoryClient) accessToken(ctx context.Context) (string, error) {
	if r.credentialFn == nil {
		return "", nil
	}
	host := normalizeRegistryHost(strings.SplitN(r.repository, "/", 2)[0])
	cred, err := r.credentialFn(ctx, orasProperties.Resource{Registry: host})
	if err != nil {
		return "", err
	}
	if cred.AccessToken != "" {
		return cred.AccessToken, nil
	}
	return cred.Password, nil
}

// NewClient creates an ORAS client for the given registry and repository.
// The registry parameter is the registry host (e.g. "ghcr.io" or "registry.example.com:5000").
// The repository parameter is the repository name (e.g. "myorg/infra-state").
func NewClient(registry, repository string, cfg Config) (*Client, error) {
	repoClient, err := newORASRepositoryClient(registry, repository, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create ORAS repository client: %w", err)
	}

	return &Client{
		repoClient:   repoClient,
		config:       cfg,
		retentionSem: make(chan struct{}, 3),
	}, nil
}

// newORASRepositoryClient creates the underlying ORAS repository client with
// the given configuration.
func newORASRepositoryClient(registry, repository string, cfg Config) (*orasRepositoryClient, error) {
	// Normalize the host (lowercase, strip scheme//v1 suffixes) so token
	// selection, the GHCR delete fallback, and credential matching all agree
	// even when the user configures "GHCR.IO" or "https://ghcr.io".
	registry = normalizeRegistryHost(registry)
	fullRef := registry + "/" + repository

	repo, err := orasRemote.NewRepository(fullRef)
	if err != nil {
		return nil, fmt.Errorf("failed to create remote repository: %w", err)
	}

	if cfg.PlainHTTP {
		repo.Registry.PlainHTTP = true
	}

	// Use provided HTTP client if set, otherwise build one
	var httpClient *http.Client
	if cfg.HTTPClient != nil {
		// Do not mutate the caller's client; clone before wrapping Transport.
		cloned := *cfg.HTTPClient
		httpClient = &cloned
	} else {
		httpClient, err = BuildHTTPClient(cfg.TLSSkipVerify, cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("failed to create HTTP client: %w", err)
		}
	}

	// Add User-Agent header for ORAS registry operations, regardless of
	// whether the client came from config or was built here.
	if _, already := httpClient.Transport.(*userAgentRoundTripper); !already {
		next := httpClient.Transport
		if next == nil {
			next = http.DefaultTransport
		}
		httpClient.Transport = &userAgentRoundTripper{
			next:      next,
			userAgent: userAgent(cfg.UserAgent),
		}
	}

	// Resolve credentials with the following priority:
	// 1. Caller-supplied CredentialFunc
	// 2. Explicit token
	// 3. Explicit username/password
	// 4. Environment variables
	//
	// Terraform/OpenTofu CLI config and Docker credential discovery belong to
	// consumers and should be supplied through CredentialFunc.
	cred := resolveCredentials(registry, cfg)

	repo.Registry.Client = &orasAuth.Client{
		Client:         httpClient,
		CredentialFunc: cred,
		Cache:          orasAuth.NewCache(),
	}

	return &orasRepositoryClient{
		repository:   fullRef,
		inner:        repo,
		credentialFn: cred,
		httpClient:   httpClient,
	}, nil
}

// tokenCredential converts a bearer-style token into a credential the
// registry can actually authenticate. GHCR rejects tokens sent raw as
// Bearer — oras-go skips the exchange when AccessToken is set — and
// instead requires basic auth against its token endpoint; any non-empty
// username is accepted (Docker CLI convention). Other registries keep the
// AccessToken as-is.
func tokenCredential(registry, token string) orasCredentials.Credential {
	if registry == "ghcr.io" {
		return orasCredentials.Credential{Username: "x", Password: token}
	}
	return orasCredentials.Credential{AccessToken: token}
}

// resolveCredentials builds a host-scoped credential function using the
// following priority:
//  0. Caller-supplied cfg.CredentialFunc
//  1. Explicit token from cfg.Token
//  2. Explicit username/password from cfg.Username/cfg.Password
//  3. ORAS_TOKEN environment variable (applies to any registry)
//  4. For ghcr.io only: GHCR_TOKEN, then GITHUB_TOKEN
//  5. Anonymous access (EmptyCredential)
func resolveCredentials(registry string, cfg Config) orasCredentials.CredentialFunc {
	// Normalize the host so case variants ("GHCR.IO") and Docker-style keys
	// ("https://ghcr.io/v1/") match the GHCR-specific paths below.
	registry = normalizeRegistryHost(registry)

	// Priority 0: caller-supplied resolver (used by OpenTofu integration).
	if cfg.CredentialFunc != nil {
		want := (orasProperties.Resource{Registry: registry}).Host()
		return func(ctx context.Context, resource orasProperties.Resource) (orasCredentials.Credential, error) {
			// v3 also challenges blob upload hosts on another registry. The
			// caller resolves a registry host's credentials, so never forward
			// them to a host this client was not configured for.
			if resource.Host() != want {
				return orasCredentials.EmptyCredential, nil
			}
			cred, err := cfg.CredentialFunc(ctx, resource.Host())
			if err != nil {
				return orasCredentials.EmptyCredential, err
			}
			return orasCredentials.Credential{
				Username:    cred.Username,
				Password:    cred.Password,
				AccessToken: cred.AccessToken,
			}, nil
		}
	}

	// Priority 1: Explicit token
	if cfg.Token != "" {
		return orasCredentials.StaticCredentialFunc(registry, tokenCredential(registry, cfg.Token))
	}

	// Priority 2: Explicit username/password
	if cfg.Username != "" || cfg.Password != "" {
		cred := orasCredentials.Credential{Username: cfg.Username, Password: cfg.Password}
		return orasCredentials.StaticCredentialFunc(registry, cred)
	}

	// Priority 3: Environment variable fallback
	if t := os.Getenv("ORAS_TOKEN"); t != "" {
		return orasCredentials.StaticCredentialFunc(registry, tokenCredential(registry, t))
	}

	// For ghcr.io, also check GHCR_TOKEN and GITHUB_TOKEN
	if registry == "ghcr.io" {
		if t := os.Getenv("GHCR_TOKEN"); t != "" {
			return orasCredentials.StaticCredentialFunc(registry, tokenCredential(registry, t))
		}
		if t := os.Getenv("GITHUB_TOKEN"); t != "" {
			return orasCredentials.StaticCredentialFunc(registry, tokenCredential(registry, t))
		}
	}

	// Fall back to anonymous access
	return orasCredentials.StaticCredentialFunc(registry, orasCredentials.EmptyCredential)
}

// userAgentRoundTripper wraps an http.RoundTripper to add a custom User-Agent
// header to every request.
type userAgentRoundTripper struct {
	next      http.RoundTripper
	userAgent string
}

// RoundTrip implements http.RoundTripper.
func (u *userAgentRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.Header.Set("User-Agent", u.userAgent)
	return u.next.RoundTrip(req)
}
