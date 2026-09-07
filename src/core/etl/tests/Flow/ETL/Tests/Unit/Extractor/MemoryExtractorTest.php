<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Tests\Double\CountingMemory;
use Flow\ETL\Tests\FlowTestCase;

use function array_map;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_memory;
use function Flow\ETL\DSL\infer_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\to_memory;
use function iterator_to_array;

final class MemoryExtractorTest extends FlowTestCase
{
    public function test_declared_schema_is_used_to_hydrate_extracted_rows(): void
    {
        $memory = new ArrayMemory();

        to_memory($memory)->load(
            rows(schema(int_schema('number'), str_schema('name')), row(['number' => 1, 'name' => 'one'])),
            flow_context(config()),
        );

        self::assertExtractedRowsEquals(
            rows(schema(str_schema('number'), str_schema('name')), row(['number' => '1', 'name' => 'one'])),
            from_memory($memory)->withSchema(schema(str_schema('number'), str_schema('name'))),
        );
    }

    public function test_memory_extractor(): void
    {
        $rows = rows(
            schema(int_schema('number'), str_schema('name')),
            row(['number' => 1, 'name' => 'one']),
            row(['number' => 2, 'name' => 'two']),
            row(['number' => 3, 'name' => 'tree']),
            row(['number' => 4, 'name' => 'four']),
            row(['number' => 5, 'name' => 'five']),
        );

        $memory = new ArrayMemory();

        to_memory($memory)->load($rows, flow_context(config()));

        $extractor = from_memory($memory);

        self::assertExtractedRowsAsArrayEquals(
            [
                ['number' => 1, 'name' => 'one'],
                ['number' => 2, 'name' => 'two'],
                ['number' => 3, 'name' => 'tree'],
                ['number' => 4, 'name' => 'four'],
                ['number' => 5, 'name' => 'five'],
            ],
            $extractor,
        );
    }

    public function test_ragged_rows_are_all_extracted_with_the_derived_schema(): void
    {
        $extractor = from_memory(new ArrayMemory([['code' => 1000], ['code' => 'AB-01'], ['id' => 3]]));

        static::assertEquals(
            [$extractor->schema(), $extractor->schema(), $extractor->schema()],
            array_map(
                static fn(Rows $rows): Schema => $rows->schema(),
                iterator_to_array($extractor->extract(flow_context(config()))),
            ),
        );
    }

    public function test_schema_before_save_does_not_freeze_empty(): void
    {
        $memory = new ArrayMemory();
        $extractor = from_memory($memory);

        static::assertSame([], $extractor->schema()->references()->names());

        $memory->save([['number' => 1, 'name' => 'one']]);

        static::assertSame(['number', 'name'], $extractor->schema()->references()->names());
        self::assertExtractedRowsAsArrayEquals([['number' => 1, 'name' => 'one']], $extractor);
    }

    public function test_a_bounded_sample_is_opt_in(): void
    {
        $memory = new ArrayMemory([['code' => 1000], ['code' => 1001], ['code' => 'AB-01']]);

        static::assertSame(
            'integer',
            from_memory($memory)->inferSchema(infer_schema()->sampleSize(2))->schema()->get('code')->type()->toString(),
        );
        static::assertSame('string', from_memory($memory)->schema()->get('code')->type()->toString());
    }

    public function test_a_mutable_memory_is_described_as_of_the_first_non_empty_schema(): void
    {
        $memory = new ArrayMemory([['number' => 1]]);
        $extractor = from_memory($memory);

        static::assertSame(['number'], $extractor->schema()->references()->names());

        $memory->save([['number' => 2, 'name' => 'two']]);

        static::assertSame(['number'], $extractor->schema()->references()->names());
    }

    public function test_infer_schema_resets_the_memo(): void
    {
        $extractor = from_memory(new ArrayMemory([['code' => 1000], ['code' => 1001], ['code' => 'AB-01']]));

        static::assertSame('string', $extractor->schema()->get('code')->type()->toString());

        $extractor->inferSchema(infer_schema()->sampleSize(2));

        static::assertSame('integer', $extractor->schema()->get('code')->type()->toString());
    }

    public function test_schema_is_memoised(): void
    {
        $extractor = from_memory(new ArrayMemory([['number' => 1]]));

        static::assertSame($extractor->schema(), $extractor->schema());
    }

    public function test_the_memory_is_dumped_once_for_the_schema_and_once_for_the_read(): void
    {
        $memory = new CountingMemory([['number' => 1], ['number' => 2]]);
        $extractor = from_memory($memory);

        $extractor->schema();
        $extractor->schema();
        iterator_to_array($extractor->extract(flow_context(config())));

        static::assertSame(2, $memory->dumps);
    }

    public function test_extract_does_not_depend_on_schema_having_been_called_first(): void
    {
        $dataset = [['code' => 1000], ['code' => 'AB-01']];

        $cold = from_memory(new ArrayMemory($dataset));
        $warm = from_memory(new ArrayMemory($dataset));
        $warm->schema();

        static::assertEquals(
            iterator_to_array($cold->extract(flow_context(config()))),
            iterator_to_array($warm->extract(flow_context(config()))),
        );
    }

    public function test_is_repeatable(): void
    {
        static::assertTrue(from_memory(new ArrayMemory([['number' => 1]]))->isRepeatable());
    }
}
