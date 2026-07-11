<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Flow\Floe\Exception\FloeException;
use Flow\Floe\SchemaEncoder;
use Flow\Floe\ValueEncoder;
use PHPUnit\Framework\TestCase;

use function array_keys;
use function array_map;
use function array_values;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class SchemaEncoderTest extends TestCase
{
    public function test_plan_captures_names_in_schema_order(): void
    {
        $plan = (new SchemaEncoder(new ValueEncoder()))->encodeSchema(schema(int_schema('id'), str_schema('name')));

        static::assertSame(['id', 'name'], array_keys($plan->columns));
        static::assertSame(
            ['id', 'name'],
            array_values(array_map(static fn($column) => $column->name, $plan->columns)),
        );
    }

    public function test_column_name_with_invalid_utf8_throws(): void
    {
        $this->expectException(FloeException::class);
        $this->expectExceptionMessage('failed to encode schema as JSON');

        (new SchemaEncoder(new ValueEncoder()))->encodeSchema(schema(str_schema("bad\xFFname")));
    }
}
