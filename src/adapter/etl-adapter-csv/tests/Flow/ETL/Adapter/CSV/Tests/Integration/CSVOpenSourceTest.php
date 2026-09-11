<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Integration;

use Flow\ETL\Adapter\CSV\CSVReadOptions;
use Flow\ETL\Adapter\CSV\Tests\Context\CSVFixtureContext;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Local\NativeLocalFilesystem;

use function array_keys;
use function array_map;
use function implode;
use function iterator_to_array;

final class CSVOpenSourceTest extends FlowTestCase
{
    public function test_close_closes_the_stream_once(): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());
        $open = CSVFixtureContext::open('two_rows.csv', $counting);

        $open->close();

        static::assertSame(1, $counting->closedStreams());
    }

    public function test_columns_of_an_empty_source_is_empty(): void
    {
        $open = CSVFixtureContext::open('empty.csv');

        try {
            static::assertSame([], $open->columns());
        } finally {
            $open->close();
        }
    }

    public function test_columns_reads_a_quoted_multiline_header_as_one_record(): void
    {
        $open = CSVFixtureContext::open('multiline_header.csv');

        try {
            static::assertSame(['id', "na\nme"], $open->columns());
        } finally {
            $open->close();
        }
    }

    public function test_columns_reports_the_resolved_header(): void
    {
        $open = CSVFixtureContext::open('header_only.csv');

        try {
            static::assertSame(['id', 'name'], $open->columns());
        } finally {
            $open->close();
        }
    }

    public function test_columns_without_a_header_line_are_generated(): void
    {
        $open = CSVFixtureContext::open('two_columns.csv', options: new CSVReadOptions(withHeader: false));

        try {
            static::assertSame(['e00', 'e01'], $open->columns());
        } finally {
            $open->close();
        }
    }

    public function test_records_yields_every_row_with_the_full_key_set(): void
    {
        $open = CSVFixtureContext::open('ragged.csv');

        try {
            $records = iterator_to_array($open->records(), false);

            static::assertCount(3, $records);

            foreach ($records as $record) {
                static::assertSame(['id', 'name', 'v'], array_keys($record->values));
            }
        } finally {
            $open->close();
        }
    }

    public function test_records_yields_one_logical_record_per_iteration(): void
    {
        $open = CSVFixtureContext::open('multiline_strings.csv');

        try {
            $records = iterator_to_array($open->records(), false);

            static::assertNotSame([], $records);
            static::assertStringContainsString("\n", implode('', array_map('strval', $records[0]->values)));
        } finally {
            $open->close();
        }
    }
}
