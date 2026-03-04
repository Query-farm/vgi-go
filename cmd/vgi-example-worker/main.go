// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/Query-farm/vgi-go/examples/scalar"
	"github.com/Query-farm/vgi-go/examples/table"
	table_in_out "github.com/Query-farm/vgi-go/examples/table_in_out"
	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
)

func main() {
	httpMode := flag.Bool("http", false, "Run as HTTP server instead of stdio")
	flag.Parse()

	w := vgi.NewWorker(
		vgi.WithCatalogName("example"),
		vgi.WithSettings(
			vgi.SettingSpec{
				Name:         "vgi_verbose_mode",
				Description:  "Enable verbose output",
				Type:         &arrow.BooleanType{},
				DefaultValue: false,
			},
			vgi.SettingSpec{
				Name:         "greeting",
				Description:  "Custom greeting message",
				Type:         arrow.BinaryTypes.String,
				DefaultValue: "Hello",
			},
			vgi.SettingSpec{
				Name:         "multiplier",
				Description:  "Value multiplier",
				Type:         arrow.PrimitiveTypes.Int64,
				DefaultValue: int64(1),
			},
			vgi.SettingSpec{
				Name:         "threshold",
				Description:  "Filter threshold",
				Type:         arrow.PrimitiveTypes.Int64,
				DefaultValue: int64(0),
			},
			vgi.SettingSpec{
				Name:        "config",
				Description: "Sequence configuration struct",
				Type: arrow.StructOf(
					arrow.Field{Name: "start", Type: arrow.PrimitiveTypes.Int64},
					arrow.Field{Name: "step", Type: arrow.PrimitiveTypes.Int64},
					arrow.Field{Name: "label", Type: arrow.BinaryTypes.String},
				),
				DefaultValue: nil,
			},
		),
	)

	// Scalar functions
	w.RegisterScalar(&scalar.AddValuesFunction{})
	w.RegisterScalar(&scalar.BernoulliFunction{})
	w.RegisterScalar(&scalar.BinaryPacketFunction{})
	w.RegisterScalar(&scalar.ConditionalMessageFunction{})
	w.RegisterScalar(&scalar.DoubleFunction{})
	w.RegisterScalar(&scalar.MultiplyBySettingFunction{})
	w.RegisterScalar(&scalar.MultiplyFunction{})
	w.RegisterScalar(&scalar.NullHandlingFunction{})
	w.RegisterScalar(&scalar.RandomBytesFunction{})
	w.RegisterScalar(&scalar.RandomIntFunction{})
	w.RegisterScalar(&scalar.ReturnSecretValueFunction{})
	w.RegisterScalar(&scalar.SumValuesFunction{})
	w.RegisterScalar(&scalar.UpperCaseFunction{})

	// Table functions
	w.RegisterTable(table.NewConstantColumnsFunction())
	w.RegisterTable(table.NewDoubleSequenceFunction())
	w.RegisterTable(table.NewGeneratorExceptionFunction())
	w.RegisterTable(table.NewLoggingGeneratorFunction())
	w.RegisterTable(table.NewNamedParamsEchoFunction())
	w.RegisterTable(table.NewNestedSequenceFunction())
	w.RegisterTable(table.NewPartitionedSequenceFunction())
	w.RegisterTable(table.NewProjectedDataFunction())
	w.RegisterTable(table.NewSequenceFunction())
	w.RegisterTable(table.NewSettingsAwareFunction())
	w.RegisterTable(table.NewStructSettingsFunction())
	w.RegisterTable(table.NewTenThousandFunction())

	// Table-in-out functions
	w.RegisterTableInOut(table_in_out.NewBufferInputFunction())
	w.RegisterTableInOut(table_in_out.NewDistributedSumFunction())
	w.RegisterTableInOut(table_in_out.NewEchoFunction())
	w.RegisterTableInOut(table_in_out.NewExceptionFinalizeFunction())
	w.RegisterTableInOut(table_in_out.NewExceptionProcessFunction())
	w.RegisterTableInOut(table_in_out.NewFilterBySettingFunction())
	w.RegisterTableInOut(table_in_out.NewRepeatInputsFunction())
	w.RegisterTableInOut(table_in_out.NewSumAllColumnsFunction())

	// Catalog tables

	// Function-backed table: columns derived from sequence's OnBind
	w.RegisterCatalogTable("data", vgi.CatalogTable{
		Name:     "large_sequence",
		Comment:  "A large sequence of integers from 0 to 1,000,000",
		Function: table.NewSequenceFunction(),
		FuncArgs: []vgi.CatalogTableArg{
			{Position: 0, Value: int64(1_000_000), Type: arrow.PrimitiveTypes.Int64},
		},
	})

	// Explicit-columns table: uses scan function handler below
	w.RegisterCatalogTable("data", vgi.CatalogTable{
		Name:    "numbers",
		Comment: "First 100 integers (demonstrates explicit columns)",
		Columns: arrow.NewSchema([]arrow.Field{
			{Name: "value", Type: arrow.PrimitiveTypes.Int64},
		}, nil),
	})

	// Views
	w.RegisterCatalogView("main", vgi.CatalogView{
		Name:       "first_ten",
		Definition: "SELECT * FROM sequence(10)",
	})
	w.RegisterCatalogView("main", vgi.CatalogView{
		Name:       "even_numbers",
		Definition: "SELECT * FROM sequence(100) WHERE n % 2 = 0",
	})
	w.RegisterCatalogView("data", vgi.CatalogView{
		Name:       "small_numbers",
		Definition: "SELECT * FROM numbers WHERE value < 10",
	})

	// Macros
	w.RegisterCatalogMacro("main", vgi.CatalogMacro{
		Name:       "vgi_multiply",
		MacroType:  vgi.MacroTypeScalar,
		Parameters: []string{"x", "y"},
		Definition: "x * y",
	})

	clampDefaults, err := vgi.BuildMacroDefaultValues([]vgi.MacroDefault{
		{Name: "lo", Value: int64(0), Type: arrow.PrimitiveTypes.Int64},
		{Name: "hi", Value: int64(100), Type: arrow.PrimitiveTypes.Int64},
	})
	if err != nil {
		panic(fmt.Sprintf("failed to build macro defaults: %v", err))
	}
	w.RegisterCatalogMacro("main", vgi.CatalogMacro{
		Name:                   "vgi_clamp",
		MacroType:              vgi.MacroTypeScalar,
		Parameters:             []string{"val", "lo", "hi"},
		ParameterDefaultValues: clampDefaults,
		Definition:             "GREATEST(lo, LEAST(hi, val))",
	})

	w.RegisterCatalogMacro("main", vgi.CatalogMacro{
		Name:       "vgi_range_table",
		MacroType:  vgi.MacroTypeTable,
		Parameters: []string{"n"},
		Definition: "SELECT * FROM range(n)",
	})

	// Handler for tables without a backing Function
	w.SetScanFunctionGetHandler(func(schemaName, tableName string) (*vgi.ScanFunctionResult, error) {
		if schemaName == "data" && tableName == "numbers" {
			return &vgi.ScanFunctionResult{
				FunctionName: "sequence",
				PositionalArguments: []vgi.ScanArg{
					{Value: int64(100), Type: arrow.PrimitiveTypes.Int64},
				},
			}, nil
		}
		return nil, fmt.Errorf("no scan function for %s.%s", schemaName, tableName)
	})

	if *httpMode {
		if err := w.RunHttp("127.0.0.1:0"); err != nil {
			log.Fatal(err)
		}
	} else {
		w.RunStdio()
	}
}
