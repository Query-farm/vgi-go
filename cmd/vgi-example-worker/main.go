// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"github.com/Query-farm/vgi-go/examples/scalar"
	"github.com/Query-farm/vgi-go/examples/table"
	table_in_out "github.com/Query-farm/vgi-go/examples/table_in_out"
	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
)

func main() {
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
	w.RegisterTable(table.NewNestedSequenceFunction())
	w.RegisterTable(table.NewPartitionedSequenceFunction())
	w.RegisterTable(table.NewProjectedDataFunction())
	w.RegisterTable(table.NewSequenceFunction())
	w.RegisterTable(table.NewSettingsAwareFunction())
	w.RegisterTable(table.NewTenThousandFunction())

	// Table-in-out functions
	w.RegisterTableInOut(table_in_out.NewBufferInputFunction())
	w.RegisterTableInOut(table_in_out.NewDistributedSumFunction())
	w.RegisterTableInOut(table_in_out.NewEchoFunction())
	w.RegisterTableInOut(table_in_out.NewExceptionFinalizeFunction())
	w.RegisterTableInOut(table_in_out.NewExceptionProcessFunction())
	w.RegisterTableInOut(table_in_out.NewRepeatInputsFunction())
	w.RegisterTableInOut(table_in_out.NewSumAllColumnsFunction())

	w.RunStdio()
}
