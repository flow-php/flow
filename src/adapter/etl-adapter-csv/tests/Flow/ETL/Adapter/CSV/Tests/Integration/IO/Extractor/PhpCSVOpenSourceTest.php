<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Integration\IO\Extractor;

use Flow\ETL\Adapter\CSV\IO\Extractor\CSVReadOptions;
use Flow\ETL\Adapter\CSV\Tests\Context\CSVFixtureContext;
use Flow\ETL\Foundation\Exception\ColumnMismatchException;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Local\NativeLocalFilesystem;

use function array_key_exists;
use function array_map;
use function array_slice;
use function count;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function implode;
use function iterator_to_array;

final class PhpCSVOpenSourceTest extends FlowTestCase
{
    public function test_close_closes_the_stream_once(): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());
        $open = CSVFixtureContext::openPhp('two_rows.csv', $counting);

        $open->close();

        static::assertSame(1, $counting->closedStreams());
    }

    public function test_columns_of_an_empty_source_is_empty(): void
    {
        $open = CSVFixtureContext::openPhp('empty.csv');

        try {
            static::assertSame([], $open->columns());
        } finally {
            $open->close();
        }
    }

    public function test_columns_reads_a_quoted_multiline_header_as_one_record(): void
    {
        $open = CSVFixtureContext::openPhp('multiline_header.csv');

        try {
            static::assertSame(['id', "na\nme"], $open->columns());
        } finally {
            $open->close();
        }
    }

    public function test_headers_are_the_header_records_resolved(): void
    {
        $open = CSVFixtureContext::openPhp('header_only.csv');

        try {
            static::assertSame([], $open->headers());
            static::assertSame([], iterator_to_array($open->records(), false));
            static::assertSame(['id', 'name'], $open->headers());
        } finally {
            $open->close();
        }
    }

    public function test_columns_reports_the_resolved_header(): void
    {
        $open = CSVFixtureContext::openPhp('header_only.csv');

        try {
            static::assertSame(['id', 'name'], $open->columns());
        } finally {
            $open->close();
        }
    }

    public function test_columns_without_a_header_line_are_generated(): void
    {
        $open = CSVFixtureContext::openPhp('two_columns.csv', options: new CSVReadOptions(withHeader: false));

        try {
            static::assertSame(['e00', 'e01'], $open->columns());
        } finally {
            $open->close();
        }
    }

    public function test_records_yields_only_the_rows_of_records_that_are_not_corrupted(): void
    {
        $open = CSVFixtureContext::openPhp('ragged.csv');

        try {
            static::assertSame([['id' => '1', 'name' => 'a', 'v' => '10']], iterator_to_array($open->records(), false));
        } finally {
            $open->close();
        }
    }

    public function test_records_yields_one_logical_record_per_iteration(): void
    {
        $open = CSVFixtureContext::openPhp('multiline_strings.csv');

        try {
            $records = iterator_to_array($open->records(), false);

            static::assertNotSame([], $records);
            static::assertStringContainsString("\n", implode('', array_map('strval', $records[0])));
        } finally {
            $open->close();
        }
    }

    public function test_produced_rows_and_bytes_exclude_the_header(): void
    {
        $open = CSVFixtureContext::openPhp('five_rows.csv');

        try {
            iterator_to_array($open->records(), false);

            static::assertSame(5, $open->producedRows());
            static::assertSame(20, $open->producedBytes());
        } finally {
            $open->close();
        }
    }

    public function test_batches_are_batch_size_rows(): void
    {
        $batches = CSVFixtureContext::batches(
            CSVFixtureContext::openPhpStream(CSVFixtureContext::content("id,name\n1,a\n2,b\n3,c\n4,d\n5,e\n")),
            schema(int_schema('id'), str_schema('name')),
            2,
        );

        static::assertIsArray($batches);
        static::assertSame([2, 2, 1], array_map(count(...), $batches));
    }

    public function test_batches_skip_undeclared_header_columns(): void
    {
        static::assertSame(
            [[['id' => 1, 'name' => 'a']]],
            CSVFixtureContext::batches(
                CSVFixtureContext::openPhpStream(CSVFixtureContext::content("id,name,extra\n1,a,x\n")),
                schema(int_schema('id'), str_schema('name')),
                10,
            ),
        );
    }

    public function test_batches_pad_a_missing_nullable_column(): void
    {
        static::assertSame(
            [[['id' => 1, 'name' => null]]],
            CSVFixtureContext::batches(
                CSVFixtureContext::openPhpStream(CSVFixtureContext::content("id\n1\n")),
                schema(int_schema('id'), str_schema('name', nullable: true)),
                10,
            ),
        );
    }

    public function test_batches_hand_up_a_record_missing_a_not_null_column(): void
    {
        $batches = CSVFixtureContext::batches(
            CSVFixtureContext::openPhpStream(CSVFixtureContext::content("id\n1\n")),
            schema(int_schema('id'), str_schema('name')),
            10,
        );

        static::assertIsArray($batches);
        static::assertSame(
            [[
                3,
                2,
                'column "'
                    . ColumnMismatchException::missingColumn('name')->column
                    . '"'
                    . ColumnMismatchException::missingColumn('name')->detail,
                '1',
                ['1'],
            ]],
            array_map(static fn(array $batch): array => array_key_exists('corrupted', $batch)
                ? array_slice($batch['corrupted'], 1)
                : $batch, $batches),
        );
    }

    public function test_batches_hand_up_corrupted_records_in_source_order(): void
    {
        $batches = CSVFixtureContext::batches(
            CSVFixtureContext::openPhpStream(CSVFixtureContext::content("id\n1\nx\n")),
            schema(int_schema('id'), str_schema('name')),
            10,
        );

        static::assertIsArray($batches);
        static::assertSame(
            [
                [
                    3,
                    2,
                    'column "'
                        . ColumnMismatchException::missingColumn('name')->column
                        . '"'
                        . ColumnMismatchException::missingColumn('name')->detail,
                    '1',
                    ['1'],
                ],
                [
                    5,
                    3,
                    'column "'
                        . ColumnMismatchException::valueDoesNotMatch('id', 'integer', 'x')->column
                        . '"'
                        . ColumnMismatchException::valueDoesNotMatch('id', 'integer', 'x')->detail,
                    'x',
                    ['x'],
                ],
            ],
            array_map(static fn(array $batch): array => array_key_exists('corrupted', $batch)
                ? array_slice($batch['corrupted'], 1)
                : $batch, $batches),
        );
    }

    public function test_batches_refusal_tie_goes_to_the_first_definition(): void
    {
        $batches = CSVFixtureContext::batches(
            CSVFixtureContext::openPhpStream(CSVFixtureContext::content("a,b\nx,y\n")),
            schema(int_schema('b'), int_schema('a')),
            10,
        );

        static::assertIsArray($batches);
        static::assertSame(
            [[
                4,
                2,
                'column "'
                    . ColumnMismatchException::valueDoesNotMatch('b', 'integer', 'y')->column
                    . '"'
                    . ColumnMismatchException::valueDoesNotMatch('b', 'integer', 'y')->detail,
                'x,y',
                ['x', 'y'],
            ]],
            array_map(static fn(array $batch): array => array_key_exists('corrupted', $batch)
                ? array_slice($batch['corrupted'], 1)
                : $batch, $batches),
        );
    }

    public function test_batches_hand_up_an_empty_to_null_null_under_not_null(): void
    {
        $batches = CSVFixtureContext::batches(
            CSVFixtureContext::openPhpStream(CSVFixtureContext::content("id,name\n1,a\n2,\n")),
            schema(int_schema('id'), str_schema('name')),
            10,
        );

        static::assertIsArray($batches);
        static::assertSame(
            [
                [
                    12,
                    3,
                    'column "'
                        . ColumnMismatchException::valueDoesNotMatch('name', 'string', null)->column
                        . '"'
                        . ColumnMismatchException::valueDoesNotMatch('name', 'string', null)->detail,
                    '2,',
                    ['2', ''],
                ],
                [['id' => 1, 'name' => 'a']],
            ],
            array_map(static fn(array $batch): array => array_key_exists('corrupted', $batch)
                ? array_slice($batch['corrupted'], 1)
                : $batch, $batches),
        );
    }

    public function test_batches_of_a_header_only_source_are_empty(): void
    {
        static::assertSame(
            [],
            CSVFixtureContext::batches(
                CSVFixtureContext::openPhpStream(CSVFixtureContext::content("id\n")),
                schema(int_schema('id'), str_schema('absent')),
                10,
            ),
        );
    }
}
