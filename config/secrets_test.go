package config

import (
	"os"
	"testing"
)

func TestResolveValue_NoPlaceholder(t *testing.T) {
	val, err := resolveValue("plain-password")
	if err != nil || val != "plain-password" {
		t.Fatalf("got %q, %v", val, err)
	}
}

func TestResolveValue_EnvVar(t *testing.T) {
	os.Setenv("TEST_DB_PASS", "secret123")
	defer os.Unsetenv("TEST_DB_PASS")

	val, err := resolveValue("${TEST_DB_PASS}")
	if err != nil {
		t.Fatal(err)
	}
	if val != "secret123" {
		t.Fatalf("expected secret123, got %q", val)
	}
}

func TestResolveValue_Default(t *testing.T) {
	os.Unsetenv("UNSET_VAR_XYZ")

	val, err := resolveValue("${UNSET_VAR_XYZ:-fallback}")
	if err != nil {
		t.Fatal(err)
	}
	if val != "fallback" {
		t.Fatalf("expected fallback, got %q", val)
	}
}

func TestResolveValue_MissingNoDefault(t *testing.T) {
	os.Unsetenv("UNSET_VAR_ABC")

	_, err := resolveValue("${UNSET_VAR_ABC}")
	if err == nil {
		t.Fatal("expected error for missing env var without default")
	}
}

func TestResolveValue_Embedded(t *testing.T) {
	os.Setenv("TEST_HOST", "db.local")
	defer os.Unsetenv("TEST_HOST")

	val, err := resolveValue("host=${TEST_HOST}:5432")
	if err != nil {
		t.Fatal(err)
	}
	if val != "host=db.local:5432" {
		t.Fatalf("expected host=db.local:5432, got %q", val)
	}
}
