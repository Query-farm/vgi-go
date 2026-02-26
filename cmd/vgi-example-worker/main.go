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
	w.RegisterTable(&table.ConstantColumnsFunction{})
	w.RegisterTable(&table.DoubleSequenceFunction{})
	w.RegisterTable(&table.GeneratorExceptionFunction{})
	w.RegisterTable(&table.LoggingGeneratorFunction{})
	w.RegisterTable(&table.NestedSequenceFunction{})
	w.RegisterTable(&table.PartitionedSequenceFunction{})
	w.RegisterTable(&table.ProjectedDataFunction{})
	w.RegisterTable(&table.SequenceFunction{})
	w.RegisterTable(&table.SettingsAwareFunction{})
	w.RegisterTable(&table.TenThousandFunction{})

	// Table-in-out functions
	w.RegisterTableInOut(&table_in_out.BufferInputFunction{})
	w.RegisterTableInOut(&table_in_out.DistributedSumFunction{})
	w.RegisterTableInOut(&table_in_out.EchoFunction{})
	w.RegisterTableInOut(&table_in_out.ExceptionFinalizeFunction{})
	w.RegisterTableInOut(&table_in_out.ExceptionProcessFunction{})
	w.RegisterTableInOut(&table_in_out.RepeatInputsFunction{})
	w.RegisterTableInOut(&table_in_out.SumAllColumnsFunction{})

	w.RunStdio()
}
