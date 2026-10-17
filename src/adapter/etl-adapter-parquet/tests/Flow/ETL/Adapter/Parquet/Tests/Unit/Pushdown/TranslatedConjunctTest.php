<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Unit\Pushdown;

use Flow\ETL\Adapter\Parquet\Pushdown\TranslatedConjunct;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\definition_from_type;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Parquet\DSL\parquet_column;
use function Flow\Types\DSL\type_numeric_string;

final class TranslatedConjunctTest extends FlowTestCase
{
    public function test_a_file_storing_every_column_as_the_body_types_it_is_read_as_is(): void
    {
        static::assertTrue((new TranslatedConjunct(parquet_column('id')->equals(1), ['id', 'name']))->readsAs(
            schema(int_schema('id'), str_schema('name')),
            schema(int_schema('id', nullable: true), str_schema('name', nullable: true)),
        ));
    }

    public function test_a_column_the_file_stores_as_another_type_is_not_read_as_is(): void
    {
        static::assertFalse((new TranslatedConjunct(parquet_column('big')->lessThan('2'), ['id', 'big']))->readsAs(
            schema(int_schema('id'), definition_from_type('big', type_numeric_string())),
            schema(int_schema('id'), str_schema('big')),
        ));
    }

    public function test_a_column_absent_from_the_file_is_read_as_is(): void
    {
        static::assertTrue((new TranslatedConjunct(parquet_column('missing')->isNull(), ['missing']))->readsAs(
            schema(int_schema('id')),
            schema(int_schema('id'), str_schema('missing', nullable: true)),
        ));
    }
}
