// Package orasstate stores Terraform/OpenTofu state in OCI registries using ORAS.
//
// It is intentionally independent from Terraform Core, OpenTofu Core, and
// plugin-framework adapters. Consumers provide state IDs and use Client for
// state, locking, workspace listing, retention, and registry compatibility.
package orasstate
