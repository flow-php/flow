<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Join\Comparison;

use Flow\ETL\Join\Comparison\Identical;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_row;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class IdenticalTest extends FlowTestCase
{
    public function test_failure(): void
    {
        static::assertFalse((new Identical('id', 'id'))->compare(
            array_to_row(['id' => 1], schema(int_schema('id'))),
            array_to_row(['id' => 2], schema(int_schema('id'))),
        ));
    }

    public function test_null_is_not_identical_to_null(): void
    {
        static::assertFalse((new Identical('id', 'id'))->compare(
            array_to_row(['id' => null], schema(str_schema('id', nullable: true))),
            array_to_row(['id' => null], schema(str_schema('id', nullable: true))),
        ));
    }

    public function test_null_is_not_identical_to_value(): void
    {
        static::assertFalse((new Identical('id', 'id'))->compare(
            array_to_row(['id' => null], schema(int_schema('id', nullable: true))),
            array_to_row(['id' => 1], schema(int_schema('id'))),
        ));
        static::assertFalse((new Identical('id', 'id'))->compare(
            array_to_row(['id' => 1], schema(int_schema('id'))),
            array_to_row(['id' => null], schema(int_schema('id', nullable: true))),
        ));
    }

    public function test_success(): void
    {
        static::assertTrue((new Identical('id', 'id'))->compare(
            array_to_row(['id' => 1], schema(int_schema('id'))),
            array_to_row(['id' => 1], schema(int_schema('id'))),
        ));
    }
}
