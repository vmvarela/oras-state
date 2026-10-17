package orasstate

import "strings"

// normalizeRegistryHost canonicalizes registry hosts used for authentication
// and GHCR-specific behavior. Credential source discovery itself belongs to
// callers via Config.CredentialFunc.
func normalizeRegistryHost(s string) string {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "https://")
	s = strings.TrimPrefix(s, "http://")
	s = strings.TrimSuffix(s, "/")
	s = strings.ToLower(s)
	if s == "index.docker.io/v1" {
		s = "index.docker.io"
	}
	return s
}
