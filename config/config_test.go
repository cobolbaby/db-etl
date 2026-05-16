package config

import (
	"log"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestFieldsMappingProjectionFromJSON(t *testing.T) {
	mapping, err := ParseFieldsMapping(`{"id":"order_id","amount":"amount","CustomerName":"customer_name"}`)
	if err != nil {
		t.Fatalf("ParseFieldsMapping returned error: %v", err)
	}

	projection, err := mapping.Projection(func(value string) string { return value }, func(value string) string { return value })
	if err != nil {
		t.Fatalf("Projection returned error: %v", err)
	}

	for _, want := range []string{
		"CustomerName AS customer_name",
		"id AS order_id",
		"amount AS amount",
	} {
		if !strings.Contains(projection, want) {
			t.Fatalf("projection %q does not contain %q", projection, want)
		}
	}
}

func TestFieldsMappingProjectionFromYAML(t *testing.T) {
	var src SourceConfig
	data := []byte(`fields_mapping:
  id: order_id
  amount: amount
  CustomerName: customer_name
`)

	if err := yaml.Unmarshal(data, &src); err != nil {
		t.Fatalf("yaml.Unmarshal returned error: %v", err)
	}

	projection, err := src.FieldsMapping.Projection(func(value string) string { return value }, func(value string) string { return value })
	if err != nil {
		t.Fatalf("Projection returned error: %v", err)
	}

	for _, want := range []string{
		"CustomerName AS customer_name",
		"id AS order_id",
		"amount AS amount",
	} {
		if !strings.Contains(projection, want) {
			t.Fatalf("projection %q does not contain %q", projection, want)
		}
	}
}

func TestFieldsMappingRejectsComplexObjectValues(t *testing.T) {
	_, err := ParseFieldsMapping(`{"order_id":{"source":"id"}}`)
	if err == nil {
		t.Fatal("expected ParseFieldsMapping to reject complex object values")
	}
}

func TestFieldsMappingRejectsNonObjectJSON(t *testing.T) {
	for _, raw := range []string{`"id"`, `["id"]`} {
		if _, err := ParseFieldsMapping(raw); err == nil {
			t.Fatalf("expected ParseFieldsMapping to reject %s", raw)
		}
	}
}

func TestFieldsMappingValidateReturnsWarnings(t *testing.T) {
	mapping := FieldsMapping{Items: map[string]string{
		"plain_field":   "target_field",
		"source.field":  "target_field_two",
		"now()":         "cdt",
		"Another-Field": "target_name_three",
	}}

	var logged strings.Builder
	originalWriter := log.Writer()
	log.SetOutput(&logged)
	defer log.SetOutput(originalWriter)

	if err := mapping.Validate(); err != nil {
		t.Fatalf("Validate returned error: %v", err)
	}
	joined := logged.String()

	if strings.Contains(joined, `source field "now()"`) {
		t.Fatalf("expression source should not warn, got %q", joined)
	}
	for _, want := range []string{
		`source field "source.field" contains special characters`,
		`source field "Another-Field" contains special characters`,
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("warnings %q does not contain %q", joined, want)
		}
	}
}

func TestFieldsMappingValidateRejectsInvalidTargetFieldName(t *testing.T) {
	mapping := FieldsMapping{Items: map[string]string{
		"id": "target-name",
	}}

	err := mapping.Validate()
	if err == nil {
		t.Fatal("expected Validate to reject invalid target field name")
	}
	if !strings.Contains(err.Error(), `"target-name" contains invalid characters`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFieldsMappingValidateRejectsReservedTargetKeyword(t *testing.T) {
	mapping := FieldsMapping{Items: map[string]string{
		"id": "select",
	}}

	err := mapping.Validate()
	if err == nil {
		t.Fatal("expected Validate to reject reserved keyword target field")
	}
	if !strings.Contains(err.Error(), `"select" is a reserved keyword`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateSourceTableNameAllowsThreePartForMSSQL(t *testing.T) {
	if err := ValidateSourceTableName("sales.dbo.orders", DBTypeMSSQL); err != nil {
		t.Fatalf("expected mssql three-part table to be allowed, got %v", err)
	}
}

func TestValidateSourceTableNameRejectsThreePartForNonMSSQL(t *testing.T) {
	for _, dbType := range []DBType{"", DBTypePG, DBTypeGP} {
		err := ValidateSourceTableName("sales.public.orders", dbType)
		if err == nil {
			t.Fatalf("expected db type %q to reject three-part table", dbType)
		}
		if !strings.Contains(err.Error(), "only supported for mssql sources") {
			t.Fatalf("unexpected error for db type %q: %v", dbType, err)
		}
	}
}
