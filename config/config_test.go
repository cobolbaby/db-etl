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
	// 告警只针对「会被 reader 当作限定名/表达式而不加引号」的源字段。
	// 含空格、连字符等字符的列名仍会被 isColumnIdentifier 识别为纯列名并加引号，
	// 属于可正常工作的场景，不应打扰用户。
	cases := []struct {
		source   string
		wantWarn bool
		reason   string
	}{
		{source: "plain_field", wantWarn: false, reason: "纯标识符"},
		{source: "now()", wantWarn: false, reason: "函数调用，圆括号已表明是表达式"},
		{source: "price - cost", wantWarn: false, reason: "算术表达式"},
		{source: "Another-Field", wantWarn: false, reason: "连字符与减号无法区分，reader 会加引号，故不告警"},
		{source: "source.field", wantWarn: true, reason: "点号会被当作限定名，不会加引号"},
		{source: "order#id", wantWarn: true, reason: "特殊字符既非表达式也非合法标识符"},
	}

	for _, tc := range cases {
		t.Run(tc.source, func(t *testing.T) {
			mapping := FieldsMapping{Items: map[string]string{tc.source: "target_field"}}

			var logged strings.Builder
			originalWriter := log.Writer()
			log.SetOutput(&logged)
			defer log.SetOutput(originalWriter)

			if err := mapping.Validate(); err != nil {
				t.Fatalf("Validate returned error: %v", err)
			}

			warned := strings.Contains(logged.String(),
				`source field "`+tc.source+`" contains special characters`)
			if warned != tc.wantWarn {
				t.Fatalf("source %q (%s): warned=%v, want %v; log=%q",
					tc.source, tc.reason, warned, tc.wantWarn, logged.String())
			}
		})
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

func TestValidateTableNameAllowsThreePartForMSSQL(t *testing.T) {
	if err := ValidateTableName("sales.dbo.orders", DBTypeMSSQL); err != nil {
		t.Fatalf("expected mssql three-part table to be allowed, got %v", err)
	}
}

func TestValidateTableNameRejectsThreePartForNonMSSQL(t *testing.T) {
	for _, dbType := range []DBType{"", DBTypePG, DBTypeGP} {
		err := ValidateTableName("sales.public.orders", dbType)
		if err == nil {
			t.Fatalf("expected db type %q to reject three-part table", dbType)
		}
		if !strings.Contains(err.Error(), "only supported for mssql") {
			t.Fatalf("unexpected error for db type %q: %v", dbType, err)
		}
	}
}
