// Package contract defines the annotation-based API between sentry-kube (or a
// human operator) and the reconciler. See docs/implementation-plan.md §3.
package contract

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	// Domain is the annotation namespace shared by all reconciler keys.
	Domain = "sts-reconciler.sentry.io"

	// DesiredSpecAnnotation carries the desired PVC spec, written by
	// sentry-kube (or kubectl) and cleared by the controller on convergence.
	DesiredSpecAnnotation = Domain + "/desired-pvc-spec"

	// StatusAnnotation tracks reconcile progress. Written and cleared only by
	// the controller.
	StatusAnnotation = Domain + "/status"

	// SkipAnnotation is an emergency opt-out: when set to "true" the
	// controller ignores the StatefulSet entirely.
	SkipAnnotation = Domain + "/skip"
)

// SupportedVersion is the only contract version this build understands.
const SupportedVersion = 1

// State enumerates state-machine positions recorded in the status annotation.
type State string

const (
	// StateBlocked: the health gate is refusing to act (e.g. no ready
	// replicas). The controller retries until the gate clears or the gate
	// timeout latches Failed.
	StateBlocked State = "Blocked"
	// StatePatching: PVC specs are being patched toward the desired spec.
	StatePatching State = "Patching"
	// StateAwaitingConvergence: PVC specs match; waiting for PVC status to
	// reflect the change (CSI ModifyVolume / resize completion).
	StateAwaitingConvergence State = "AwaitingConvergence"
	// StateDeleting: PVCs converged; the StatefulSet is being orphan-deleted
	// so it can be recreated with matching volumeClaimTemplates.
	StateDeleting State = "Deleting"
	// StateFailed: a terminal validation or convergence failure. The
	// controller stops acting until the desired spec changes.
	StateFailed State = "Failed"
	// StateDryRun: the controller is in dry-run mode and only records what it
	// would do.
	StateDryRun State = "DryRun"
)

// DesiredSpec is the parsed desired-pvc-spec annotation.
type DesiredSpec struct {
	Version int                     `json:"version"`
	Claims  map[string]ClaimDesired `json:"claims"`

	// Raw is the annotation value the spec was parsed from; used for hashing.
	Raw string `json:"-"`
}

// ClaimDesired is the desired state for one volumeClaimTemplate.
type ClaimDesired struct {
	// VolumeAttributesClassName is the target VAC, if a VAC change is wanted.
	VolumeAttributesClassName *string `json:"volumeAttributesClassName,omitempty"`
	// Storage is the target resources.requests.storage, if expansion is
	// wanted. Kubernetes only supports expansion, never shrinking.
	Storage *resource.Quantity `json:"storage,omitempty"`
}

// Status is the parsed status annotation.
type Status struct {
	Version          int               `json:"version"`
	State            State             `json:"state"`
	ObservedSpecHash string            `json:"observedSpecHash"`
	PVCs             map[string]string `json:"pvcs,omitempty"`
	Reason           string            `json:"reason,omitempty"`
	LastTransition   time.Time         `json:"lastTransition"`
}

// ParseDesiredSpec parses and validates the desired-pvc-spec annotation value.
// Unknown fields anywhere in the document are rejected: the controller must
// never act on a partial interpretation of the operator's intent.
func ParseDesiredSpec(value string) (*DesiredSpec, error) {
	dec := json.NewDecoder(bytes.NewReader([]byte(value)))
	dec.DisallowUnknownFields()
	var spec DesiredSpec
	if err := dec.Decode(&spec); err != nil {
		return nil, fmt.Errorf("invalid JSON in %s: %w", DesiredSpecAnnotation, err)
	}
	if dec.More() {
		return nil, fmt.Errorf("invalid JSON in %s: trailing data after document", DesiredSpecAnnotation)
	}
	if spec.Version != SupportedVersion {
		return nil, fmt.Errorf("unsupported contract version %d (supported: %d)", spec.Version, SupportedVersion)
	}
	if len(spec.Claims) == 0 {
		return nil, fmt.Errorf("%s must list at least one claim", DesiredSpecAnnotation)
	}
	for name, claim := range spec.Claims {
		if claim.VolumeAttributesClassName == nil && claim.Storage == nil {
			return nil, fmt.Errorf("claim %q requests no changes; at least one of volumeAttributesClassName or storage is required", name)
		}
		if claim.VolumeAttributesClassName != nil && *claim.VolumeAttributesClassName == "" {
			return nil, fmt.Errorf("claim %q: volumeAttributesClassName may not be empty (clearing a VAC is not supported)", name)
		}
		if claim.Storage != nil && claim.Storage.Sign() <= 0 {
			return nil, fmt.Errorf("claim %q: storage must be a positive quantity, got %s", name, claim.Storage.String())
		}
	}
	spec.Raw = value
	return &spec, nil
}

// Hash returns a stable content hash of the desired spec annotation, used to
// detect mid-flight spec changes.
func (d *DesiredSpec) Hash() string {
	return HashValue(d.Raw)
}

// HashValue hashes a raw annotation value. Exposed so failures can be latched
// against spec content that did not even parse.
func HashValue(value string) string {
	sum := sha256.Sum256([]byte(value))
	return "sha256:" + hex.EncodeToString(sum[:])
}

// ClaimNames returns the claim names in stable order.
func (d *DesiredSpec) ClaimNames() []string {
	names := make([]string, 0, len(d.Claims))
	for name := range d.Claims {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// ParseStatus parses the status annotation. A missing or corrupt status is
// not fatal — the controller recomputes the world every reconcile — so
// callers treat an error as "no status".
func ParseStatus(value string) (*Status, error) {
	var st Status
	if err := json.Unmarshal([]byte(value), &st); err != nil {
		return nil, fmt.Errorf("invalid JSON in %s: %w", StatusAnnotation, err)
	}
	return &st, nil
}

// Encode serializes the status for storage in the annotation.
func (s *Status) Encode() (string, error) {
	b, err := json.Marshal(s)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
