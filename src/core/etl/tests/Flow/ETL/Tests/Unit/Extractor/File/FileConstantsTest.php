<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor\File;

use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Extractor\File\FileConstants;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class FileConstantsTest extends FlowTestCase
{
    public function test_values_are_empty_without_metadata_columns_and_partitions(): void
    {
        static::assertSame([], (new FileConstants(null, [], []))->values());
    }

    public function test_values_carry_the_uri_when_metadata_columns_are_on(): void
    {
        static::assertSame(
            ['_input_file_uri' => 'memory://orders/data.csv'],
            (new FileConstants('memory://orders/data.csv', [], []))->values(),
        );
    }

    public function test_values_carry_every_partition_and_null_for_one_the_path_lacks(): void
    {
        static::assertSame(
            ['_input_file_uri' => 'memory://orders/year=2024/data.csv', 'year' => 2024, 'month' => null],
            (new FileConstants(
                'memory://orders/year=2024/data.csv',
                ['year' => false, 'month' => true],
                ['year' => 2024],
            ))->values(),
        );
    }

    public function test_fill_rows_returns_a_batch_with_nothing_to_add_as_it_is(): void
    {
        $rows = array_to_rows([['name' => 'Norbert']], schema(str_schema('name')));

        static::assertSame($rows, (new FileConstants(null, [], []))->fillRows(
            $rows,
            $rows->schema(),
            new PhpBackend(),
        ));
    }

    public function test_fill_rows_adds_the_constants_to_every_row_under_the_declared_schema(): void
    {
        $declared = schema(str_schema('name'), str_schema('_input_file_uri'), int_schema('year'));

        static::assertEquals(
            array_to_rows([
                ['name' => 'Norbert', '_input_file_uri' => 'memory://orders/data.csv', 'year' => 2024],
                ['name' => 'Flow', '_input_file_uri' => 'memory://orders/data.csv', 'year' => 2024],
            ], $declared),
            (new FileConstants('memory://orders/data.csv', ['year' => false], ['year' => 2024]))->fillRows(
                array_to_rows([
                    ['name' => 'Norbert'],
                    ['name' => 'Flow'],
                ], schema(str_schema('name'))),
                $declared,
                new PhpBackend(),
            ),
        );
    }
}
