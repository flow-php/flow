<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Unit;

use Flow\ETL\Adapter\PostgreSql\PostgreSqlEncoder;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class PostgreSqlEncoderTest extends FlowTestCase
{
    public function test_encode_returns_each_row_as_its_value_map(): void
    {
        static::assertSame(
            [['id' => 1, 'name' => 'Norbert'], ['id' => 2, 'name' => null]],
            (new PostgreSqlEncoder())->encode(array_to_rows(
                [['id' => 1, 'name' => 'Norbert'], ['id' => 2, 'name' => null]],
                schema(int_schema('id'), str_schema('name', nullable: true)),
            )),
        );
    }

    public function test_encode_returns_the_original_value_maps(): void
    {
        static::assertSame(
            [['id' => 1, 'active' => true], ['id' => 2, 'active' => false]],
            (new PostgreSqlEncoder())->encode(array_to_rows(
                [['id' => 1, 'active' => true], ['id' => 2, 'active' => false]],
                schema(int_schema('id'), bool_schema('active')),
            )),
        );
    }

    public function test_encode_of_an_empty_batch_returns_empty(): void
    {
        static::assertSame([], (new PostgreSqlEncoder())->encode(array_to_rows([], schema(int_schema('id')))));
    }
}
