<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row;

use Flow\ETL\Row\InferredBatch;
use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class InferredBatchTest extends FlowTestCase
{
    public function test_a_column_absent_from_one_row_is_nullable(): void
    {
        static::assertEquals(
            schema(int_schema('id'), str_schema('name', nullable: true)),
            (new InferredBatch())->of([
                new RawRowValues(['id' => 1, 'name' => 'a']),
                new RawRowValues(['id' => 2]),
            ])->schema(),
        );
    }

    public function test_an_empty_batch_infers_an_empty_schema(): void
    {
        static::assertEquals(rows(schema()), (new InferredBatch())->of([]));
    }

    public function test_an_explicit_null_makes_the_column_nullable(): void
    {
        static::assertEquals(
            schema(int_schema('id'), str_schema('name', nullable: true)),
            (new InferredBatch())->of([
                new RawRowValues(['id' => 1, 'name' => 'a']),
                new RawRowValues(['id' => 2, 'name' => null]),
            ])->schema(),
        );
    }

    public function test_metadata_travels_from_the_raw_values_onto_the_definition(): void
    {
        static::assertSame(
            ['origin' => 'csv'],
            (new InferredBatch())
                ->of([
                    new RawRowValues(['id' => 1], ['id' => Metadata::fromArray(['origin' => 'csv'])]),
                ])
                ->schema()
                ->get('id')
                ->metadata()
                ->normalize(),
        );
    }

    public function test_rows_keep_their_values_verbatim(): void
    {
        static::assertEquals(
            rows(schema(int_schema('id'), str_schema('name')), row(['id' => 1, 'name' => 'a'])),
            (new InferredBatch())->of([new RawRowValues(['id' => 1, 'name' => 'a'])]),
        );
    }

    public function test_widening_folds_int_and_string_into_one_column(): void
    {
        static::assertSame(
            'string',
            (new InferredBatch())
                ->of([
                    new RawRowValues(['v' => 1]),
                    new RawRowValues(['v' => 'text']),
                ])
                ->schema()
                ->get('v')
                ->type()
                ->toString(),
        );
    }
}
