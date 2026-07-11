<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Config\Telemetry;

use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\Tests\FlowTestCase;
use ReflectionClass;

final class TelemetryAttributesTest extends FlowTestCase
{
    public function test_all_custom_keys_use_the_flow_etl_prefix(): void
    {
        // @mago-expect analysis:mixed-assignment
        foreach ((new ReflectionClass(TelemetryAttributes::class))->getConstants() as $name => $value) {
            static::assertIsString($value);
            static::assertStringStartsWith(
                'flow.etl.',
                $value,
                "Constant {$name} must not extend a reserved OTel namespace",
            );
        }
    }

    public function test_dataframe_attributes_are_defined(): void
    {
        static::assertSame('flow.etl.dataframe.id', TelemetryAttributes::ATTR_DATAFRAME_ID);
        static::assertSame('flow.etl.dataframe.name', TelemetryAttributes::ATTR_DATAFRAME_NAME);
        static::assertSame('flow.etl.memory.max', TelemetryAttributes::ATTR_MEMORY_MAX);
        static::assertSame('flow.etl.memory.min', TelemetryAttributes::ATTR_MEMORY_MIN);
        static::assertSame('flow.etl.rows.throughput.per_second', TelemetryAttributes::ATTR_ROWS_THROUGHPUT);
        static::assertSame('flow.etl.rows.total', TelemetryAttributes::ATTR_ROWS_TOTAL);
    }

    public function test_loading_attributes_are_defined(): void
    {
        static::assertSame('flow.etl.destination.uri', TelemetryAttributes::ATTR_LOADER_DESTINATION_URI);
        static::assertSame('flow.etl.loader.class', TelemetryAttributes::ATTR_LOADER_CLASS);
        static::assertSame('flow.etl.loading.rows', TelemetryAttributes::ATTR_LOADING_ROWS);
    }

    public function test_transformation_attributes_are_defined(): void
    {
        static::assertSame('flow.etl.join.type', TelemetryAttributes::ATTR_JOIN_TYPE);
        static::assertSame('flow.etl.scalar.function', TelemetryAttributes::ATTR_SCALAR_FUNCTION);
        static::assertSame('flow.etl.transformation.input_rows', TelemetryAttributes::ATTR_TRANSFORMATION_INPUT_ROWS);
        static::assertSame('flow.etl.transformation.output_rows', TelemetryAttributes::ATTR_TRANSFORMATION_OUTPUT_ROWS);
        static::assertSame('flow.etl.transformer.class', TelemetryAttributes::ATTR_TRANSFORMER_CLASS);
    }
}
