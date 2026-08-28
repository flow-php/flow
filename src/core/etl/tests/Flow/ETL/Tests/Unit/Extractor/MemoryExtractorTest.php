<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Tests\FlowTestCase;

use function array_map;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_memory;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\to_memory;
use function iterator_to_array;

final class MemoryExtractorTest extends FlowTestCase
{
    public function test_declared_schema_is_used_to_hydrate_extracted_rows(): void
    {
        $memory = new ArrayMemory();

        to_memory($memory)->load(rows(row(int_entry('number', 1), str_entry('name', 'one'))), flow_context(config()));

        self::assertExtractedRowsEquals(
            rows(row(str_entry('number', '1'), str_entry('name', 'one'))),
            from_memory($memory)->withSchema(schema(str_schema('number'), str_schema('name'))),
        );
    }

    public function test_memory_extractor(): void
    {
        $rows = rows(
            row(int_entry('number', 1), str_entry('name', 'one')),
            row(int_entry('number', 2), str_entry('name', 'two')),
            row(int_entry('number', 3), str_entry('name', 'tree')),
            row(int_entry('number', 4), str_entry('name', 'four')),
            row(int_entry('number', 5), str_entry('name', 'five')),
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
}
