<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Integration\IO\Inference;

use Flow\ETL\Adapter\JSON\IO\Inference\JsonFileSample;
use Flow\ETL\Adapter\JSON\IO\JsonFraming;
use Flow\ETL\Adapter\JSON\Tests\Context\JsonFixtureContext;
use Flow\ETL\IO\Extractor\File\SourceFile;
use Flow\ETL\Tests\Context\MemoryFiles;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\Filesystem\DSL\path;
use function iterator_to_array;

final class JsonFileSampleTest extends FlowTestCase
{
    public function test_a_blank_line_counts_its_bytes_but_no_row(): void
    {
        $sample = new JsonFileSample(
            JsonFixtureContext::reader(JsonFraming::Lines),
            JsonFixtureContext::source('blank_line.jsonl'),
        );

        iterator_to_array($sample, false);

        static::assertSame([19, true], [$sample->bytes(), $sample->complete()]);
    }

    public function test_a_document_counts_rows_and_no_bytes(): void
    {
        $sample = new JsonFileSample(JsonFixtureContext::reader(), JsonFixtureContext::source('five_rows.json'));

        iterator_to_array($sample, false);

        static::assertSame([0, true], [$sample->bytes(), $sample->complete()]);
    }

    public function test_a_last_line_without_a_line_ending_counts_one_byte_more(): void
    {
        $sample = new JsonFileSample(
            JsonFixtureContext::reader(JsonFraming::Lines, MemoryFiles::with(['memory://source.jsonl' => '{"id":1}'])),
            new SourceFile(path('memory://source.jsonl')),
        );

        iterator_to_array($sample, false);

        static::assertSame([9, true], [$sample->bytes(), $sample->complete()]);
    }

    public function test_a_sample_stopped_early_records_what_it_read(): void
    {
        $sample = new JsonFileSample(
            JsonFixtureContext::reader(JsonFraming::Lines),
            JsonFixtureContext::source('five_rows.jsonl'),
        );

        foreach ($sample as $index => $_row) {
            if ($index === 1) {
                break;
            }
        }

        static::assertSame([18, false], [$sample->bytes(), $sample->complete()]);
    }

    public function test_a_whole_file_records_every_row_and_byte(): void
    {
        $sample = new JsonFileSample(
            JsonFixtureContext::reader(JsonFraming::Lines),
            JsonFixtureContext::source('five_rows.jsonl'),
        );

        iterator_to_array($sample, false);

        static::assertSame([45, true], [$sample->bytes(), $sample->complete()]);
    }

    public function test_nothing_is_recorded_before_the_sample_is_iterated(): void
    {
        $sample = new JsonFileSample(
            JsonFixtureContext::reader(JsonFraming::Lines),
            JsonFixtureContext::source('five_rows.jsonl'),
        );

        static::assertSame([0, false], [$sample->bytes(), $sample->complete()]);
    }
}
