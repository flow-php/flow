<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use Flow\ETL\Extractor\FileConstants;
use Flow\ETL\Extractor\PartitionColumns;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\memory_filesystem;

final class FileConstantsTest extends FlowTestCase
{
    public function test_a_source_column_named_input_file_uri_keeps_its_body_position(): void
    {
        static::assertSame(
            ['_input_file_uri' => 'memory://orders/data.csv', 'name' => 'Norbert'],
            (new FileConstants(new PartitionColumns(memory_filesystem()), 'memory://orders/data.csv', [], []))->fill([
                '_input_file_uri' => 'hijacked',
                'name' => 'Norbert',
            ]),
        );
    }

    public function test_partition_columns_leave_their_body_position_and_are_re_appended(): void
    {
        static::assertSame(
            ['name' => 'Norbert', 'year' => 2024],
            (new FileConstants(
                new PartitionColumns(memory_filesystem()),
                null,
                ['year' => false],
                ['year' => 2024],
            ))->fill(['year' => 'from the body', 'name' => 'Norbert']),
        );
    }

    public function test_the_uri_is_stamped_only_when_metadata_columns_are_on(): void
    {
        static::assertSame(
            ['name' => 'Norbert'],
            (new FileConstants(new PartitionColumns(memory_filesystem()), null, [], []))->fill(['name' => 'Norbert']),
        );
    }

    public function test_the_uri_joins_the_body_when_metadata_columns_are_on(): void
    {
        static::assertSame(
            ['name' => 'Norbert', '_input_file_uri' => 'memory://orders/data.csv'],
            (new FileConstants(new PartitionColumns(memory_filesystem()), 'memory://orders/data.csv', [], []))->fill([
                'name' => 'Norbert',
            ]),
        );
    }

    public function test_fill_rows_returns_a_batch_with_nothing_to_add_as_it_is(): void
    {
        $rows = rows(schema(str_schema('name')), row(['name' => 'Norbert']));

        static::assertSame($rows, (new FileConstants(
            new PartitionColumns(memory_filesystem()),
            null,
            [],
            [],
        ))->fillRows($rows, $rows->schema()));
    }

    public function test_fill_rows_adds_the_constants_to_every_row_under_the_declared_schema(): void
    {
        $declared = schema(str_schema('name'), str_schema('_input_file_uri'), int_schema('year'));

        static::assertEquals(
            rows(
                $declared,
                row(['name' => 'Norbert', '_input_file_uri' => 'memory://orders/data.csv', 'year' => 2024]),
                row(['name' => 'Flow', '_input_file_uri' => 'memory://orders/data.csv', 'year' => 2024]),
            ),
            (new FileConstants(
                new PartitionColumns(memory_filesystem()),
                'memory://orders/data.csv',
                ['year' => false],
                ['year' => 2024],
            ))->fillRows(
                rows(schema(str_schema('name')), row(['name' => 'Norbert']), row(['name' => 'Flow'])),
                $declared,
            ),
        );
    }
}
