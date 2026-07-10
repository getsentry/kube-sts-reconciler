package contract

import (
	"strings"
	"testing"
)

func TestParseDesiredSpecValid(t *testing.T) {
	raw := `{
		"version": 1,
		"claims": {
			"sqlite": {
				"volumeAttributesClassName": "vac-i15000t140",
				"storage": "333Gi"
			},
			"logs": {"storage": "10Gi"}
		}
	}`
	spec, err := ParseDesiredSpec(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := spec.ClaimNames(); len(got) != 2 || got[0] != "logs" || got[1] != "sqlite" {
		t.Fatalf("ClaimNames() = %v, want [logs sqlite]", got)
	}
	sqlite := spec.Claims["sqlite"]
	if sqlite.VolumeAttributesClassName == nil || *sqlite.VolumeAttributesClassName != "vac-i15000t140" {
		t.Errorf("sqlite VAC = %v", sqlite.VolumeAttributesClassName)
	}
	if sqlite.Storage == nil || sqlite.Storage.String() != "333Gi" {
		t.Errorf("sqlite storage = %v", sqlite.Storage)
	}
	if spec.Hash() != HashValue(raw) {
		t.Error("Hash() should be stable over the raw annotation value")
	}
}

func TestParseDesiredSpecRejects(t *testing.T) {
	cases := map[string]struct {
		raw     string
		wantErr string
	}{
		"not json":          {`nope`, "invalid JSON"},
		"trailing garbage":  {`{"version":1,"claims":{"a":{"storage":"1Gi"}}} extra`, "trailing data"},
		"unknown top field": {`{"version":1,"claims":{"a":{"storage":"1Gi"}},"mystery":true}`, "unknown field"},
		"unknown claim field": {
			`{"version":1,"claims":{"a":{"storage":"1Gi","storageClassName":"fast"}}}`,
			"unknown field",
		},
		"wrong version": {`{"version":2,"claims":{"a":{"storage":"1Gi"}}}`, "unsupported contract version"},
		"no version":    {`{"claims":{"a":{"storage":"1Gi"}}}`, "unsupported contract version"},
		"no claims":     {`{"version":1,"claims":{}}`, "at least one claim"},
		"empty claim":   {`{"version":1,"claims":{"a":{}}}`, "requests no changes"},
		"empty vac":     {`{"version":1,"claims":{"a":{"volumeAttributesClassName":""}}}`, "may not be empty"},
		"zero storage":  {`{"version":1,"claims":{"a":{"storage":"0"}}}`, "positive quantity"},
		"negative":      {`{"version":1,"claims":{"a":{"storage":"-5Gi"}}}`, "positive quantity"},
		"bad quantity":  {`{"version":1,"claims":{"a":{"storage":"333GiB"}}}`, "invalid JSON"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := ParseDesiredSpec(tc.raw)
			if err == nil {
				t.Fatal("expected an error")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error %q does not contain %q", err, tc.wantErr)
			}
		})
	}
}

func TestStatusRoundTrip(t *testing.T) {
	st := &Status{
		Version:          1,
		State:            StateAwaitingConvergence,
		ObservedSpecHash: "sha256:abc",
		PVCs:             map[string]string{"sqlite-x-0": "Converged"},
		Reason:           "",
	}
	encoded, err := st.Encode()
	if err != nil {
		t.Fatal(err)
	}
	back, err := ParseStatus(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if back.State != st.State || back.ObservedSpecHash != st.ObservedSpecHash || back.PVCs["sqlite-x-0"] != "Converged" {
		t.Fatalf("round trip mismatch: %+v", back)
	}
}
