package writer

import (
	"strings"
	"testing"

	"db-etl/config"
)

func TestSourceIdentityAllowsThreePartTableForMSSQL(t *testing.T) {
	source := &config.SourceConfig{
		Table:  "sales.dbo.orders",
		DBType: config.DBTypeMSSQL,
	}

	identity, err := sourceIdentity(source)
	if err != nil {
		t.Fatalf("expected mssql three-part table to be allowed, got %v", err)
	}

	if identity.Database != "sales" || identity.Schema != "dbo" || identity.Table != "orders" {
		t.Fatalf("unexpected source identity: %#v", identity)
	}
}

func TestSourceIdentityRejectsThreePartTableForNonMSSQL(t *testing.T) {
	source := &config.SourceConfig{
		Table:  "sales.public.orders",
		DBType: config.DBTypePG,
	}

	_, err := sourceIdentity(source)
	if err == nil {
		t.Fatal("expected non-mssql three-part table to be rejected")
	}
	if !strings.Contains(err.Error(), "only supported for mssql sources") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestTargetIdentityRejectsThreePartTable(t *testing.T) {
	target := &config.TargetConfig{Table: "sales.public.orders"}

	_, err := targetIdentity(target)
	if err == nil {
		t.Fatal("expected three-part target table to be rejected")
	}
	if !strings.Contains(err.Error(), "target table must be table or schema.table") {
		t.Fatalf("unexpected error: %v", err)
	}
}