<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use Flow\ETL\Extractor\FileConstants;
use Flow\ETL\Extractor\PartitionColumns;
use Flow\ETL\Tests\FlowTestCase;

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
}
