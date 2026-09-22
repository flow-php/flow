<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Integration\JSONMachine;

use Flow\ETL\Adapter\JSON\JSONMachine\JsonFileSample;
use Flow\ETL\Adapter\JSON\JSONMachine\JsonFormat;
use Flow\ETL\Adapter\JSON\JSONMachine\JsonSampledRows;
use Flow\ETL\Adapter\JSON\Tests\Context\JsonFixtureContext;
use Flow\ETL\Extractor\SourceFile;
use Flow\ETL\Tests\Context\MemoryFiles;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\Filesystem\DSL\path;
use function iterator_to_array;

final class JsonFileSampleTest extends FlowTestCase
{
    public function test_a_blank_line_counts_its_bytes_but_no_row(): void
    {
        $sample = new JsonFileSample(
            JsonFixtureContext::reader(JsonFormat::Lines),
            JsonFixtureContext::source('blank_line.jsonl'),
        );

        iterator_to_array($sample, false);

        static::assertEquals(new JsonSampledRows(2, 19, true), $sample->sampled());
    }

    public function test_a_document_counts_rows_and_no_bytes(): void
    {
        $sample = new JsonFileSample(JsonFixtureContext::reader(), JsonFixtureContext::source('five_rows.json'));

        iterator_to_array($sample, false);

        static::assertEquals(new JsonSampledRows(5, 0, true), $sample->sampled());
    }

    public function test_a_last_line_without_a_line_ending_counts_one_byte_more(): void
    {
        $sample = new JsonFileSample(
            JsonFixtureContext::reader(JsonFormat::Lines, MemoryFiles::with(['memory://source.jsonl' => '{"id":1}'])),
            new SourceFile(path('memory://source.jsonl')),
        );

        iterator_to_array($sample, false);

        static::assertEquals(new JsonSampledRows(1, 9, true), $sample->sampled());
    }

    public function test_a_sample_stopped_early_records_what_it_read(): void
    {
        $sample = new JsonFileSample(
            JsonFixtureContext::reader(JsonFormat::Lines),
            JsonFixtureContext::source('five_rows.jsonl'),
        );

        foreach ($sample as $index => $_row) {
            if ($index === 1) {
                break;
            }
        }

        static::assertEquals(new JsonSampledRows(2, 18, false), $sample->sampled());
    }

    public function test_a_whole_file_records_every_row_and_byte(): void
    {
        $sample = new JsonFileSample(
            JsonFixtureContext::reader(JsonFormat::Lines),
            JsonFixtureContext::source('five_rows.jsonl'),
        );

        iterator_to_array($sample, false);

        static::assertEquals(new JsonSampledRows(5, 45, true), $sample->sampled());
    }

    public function test_nothing_is_recorded_before_the_sample_is_iterated(): void
    {
        static::assertNull(
            (new JsonFileSample(
                JsonFixtureContext::reader(JsonFormat::Lines),
                JsonFixtureContext::source('five_rows.jsonl'),
            ))->sampled(),
        );
    }
}
