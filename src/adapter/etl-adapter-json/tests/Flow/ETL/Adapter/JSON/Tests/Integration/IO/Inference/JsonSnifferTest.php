<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Integration\IO\Inference;

use Flow\ETL\Adapter\JSON\IO\Inference\JsonSniffer;
use Flow\ETL\Adapter\JSON\IO\JsonFraming;
use Flow\ETL\Adapter\JSON\Tests\Context\JsonFixtureContext;
use Flow\ETL\IO\Extractor\File\SourceFile;
use Flow\ETL\IO\Inference\SchemaInference;
use Flow\ETL\IO\Inference\SchemaInferrer;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\Context\MemoryFiles;
use Flow\ETL\Tests\Context\ReversedSniffing;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use PHPUnit\Framework\Attributes\TestWith;

use function Flow\Filesystem\DSL\path;

final class JsonSnifferTest extends FlowTestCase
{
    public function test_every_listed_file_is_one_unit_and_json_carries_no_header(): void
    {
        $sniffer = JsonFixtureContext::sniffer(JsonFraming::Array, JsonFixtureContext::sources(
            'five_rows.json',
            'parity_people.json',
        ));

        static::assertSame(2, $sniffer->units());
        static::assertSame([], $sniffer->names());
    }

    public function test_no_file_is_opened_before_a_sniff(): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());

        JsonFixtureContext::sniffer(
            JsonFraming::Array,
            JsonFixtureContext::sources('five_rows.json', 'parity_people.json'),
            $counting,
        )->units();

        static::assertSame(0, $counting->readFromCalls);
    }

    #[TestWith([JsonFraming::Array, 'five_rows.json'])]
    #[TestWith([JsonFraming::Lines, 'five_rows.jsonl'])]
    public function test_a_sniff_folds_at_most_its_budget(JsonFraming $format, string $fixture): void
    {
        $sniffer = JsonFixtureContext::sniffer($format, JsonFixtureContext::sources($fixture));

        static::assertSame(2, $sniffer->sniff(0, 2)->columns->rows);
        static::assertSame(5, $sniffer->sniff(0, -1)->columns->rows);
    }

    public function test_a_sniff_opens_only_its_own_unit_and_closes_it(): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());

        JsonFixtureContext::sniffer(
            JsonFraming::Array,
            JsonFixtureContext::globSources('extra_key/*.json', $counting),
            $counting,
        )->sniff(1, -1);

        static::assertSame(1, $counting->readFromCalls);
        static::assertSame(1, $counting->closedStreams());
    }

    public function test_a_sniff_reports_the_rows_bytes_and_completeness_of_its_unit(): void
    {
        $sniffer = JsonFixtureContext::sniffer(JsonFraming::Lines, JsonFixtureContext::sources(
            'five_rows.jsonl',
            'blank_line.jsonl',
        ));

        $first = $sniffer->sniff(0, -1);
        $second = $sniffer->sniff(1, -1);

        static::assertSame([5, 45, true], [$first->columns->rows, $first->bytes, $first->complete]);
        static::assertSame([2, 19, true], [$second->columns->rows, $second->bytes, $second->complete]);
    }

    public function test_a_unit_sniffer_sniffs_the_same_as_the_parent(): void
    {
        $sniffer = JsonFixtureContext::sniffer(JsonFraming::Lines, JsonFixtureContext::sources(
            'five_rows.jsonl',
            'blank_line.jsonl',
        ));
        $unit = $sniffer->parts(1, 8 << 20)[0];

        static::assertInstanceOf(JsonSniffer::class, $unit);
        static::assertSame(1, $unit->units());
        static::assertEquals($sniffer->sniff(1, -1), $unit->sniff(0, -1));
    }

    public function test_a_unit_sniffer_rebuilt_from_its_description_sniffs_the_same(): void
    {
        $sniffer = JsonFixtureContext::sniffer(JsonFraming::Lines, JsonFixtureContext::sources(
            'five_rows.jsonl',
            'blank_line.jsonl',
        ));
        $unit = $sniffer->parts(1, 8 << 20)[0];

        static::assertInstanceOf(JsonSniffer::class, $unit);

        $description = $unit->describe();

        static::assertNotNull($description);
        static::assertTrue($description->sensitive());
        static::assertEquals($sniffer->sniff(1, -1), DescribeRoundTrip::rebuilt($unit)->sniff(0, -1));
    }

    public function test_a_document_or_a_lines_file_that_fits_is_one_part(): void
    {
        static::assertCount(1, JsonFixtureContext::sniffer(
            JsonFraming::Array,
            JsonFixtureContext::globSources('timezones.json'),
        )->parts(0, 16));
        static::assertCount(1, JsonFixtureContext::sniffer(
            JsonFraming::Lines,
            JsonFixtureContext::globSources('timezones.jsonl'),
        )->parts(0, 8 << 20));
        static::assertCount(1, JsonFixtureContext::sniffer(
            JsonFraming::Lines,
            JsonFixtureContext::sources('timezones.jsonl'),
        )->parts(0, 16));
    }

    public function test_a_large_lines_file_is_cut_into_window_parts_whose_samples_merge_to_the_whole_sniff(): void
    {
        $sniffer = JsonFixtureContext::sniffer(JsonFraming::Lines, JsonFixtureContext::globSources('timezones.jsonl'));
        $parts = $sniffer->parts(0, 8192);

        static::assertCount(4, $parts);

        $merged = null;
        $bytes = 0;

        foreach ($parts as $part) {
            static::assertSame(1, $part->units());
            $bytes += $part->bytes(0);
            $sample = $part->sniff(0, -1);
            $merged = $merged === null ? $sample : $merged->merge($sample);
        }

        static::assertSame($sniffer->bytes(0), $bytes);
        static::assertEquals($sniffer->sniff(0, -1), $merged);
    }

    public function test_a_window_part_rebuilt_from_its_description_sniffs_the_same(): void
    {
        $part = JsonFixtureContext::sniffer(
            JsonFraming::Lines,
            JsonFixtureContext::globSources('timezones.jsonl'),
        )->parts(0, 8192)[2];

        static::assertInstanceOf(JsonSniffer::class, $part);

        static::assertNotNull($part->describe());
        static::assertEquals($part->sniff(0, -1), DescribeRoundTrip::rebuilt($part)->sniff(0, -1));
    }

    public function test_a_sniffer_over_a_memory_filesystem_does_not_describe(): void
    {
        static::assertNull(
            JsonFixtureContext::sniffer(
                JsonFraming::Lines,
                [new SourceFile(path('memory://source.jsonl'))],
                MemoryFiles::with(['memory://source.jsonl' => '{"id":1}']),
            )->describe(),
        );
    }

    public function test_bytes_are_the_listed_size(): void
    {
        static::assertSame(
            45,
            JsonFixtureContext::sniffer(JsonFraming::Lines, JsonFixtureContext::globSources('five_rows.jsonl'))->bytes(
                0,
            ),
        );
    }

    /**
     * @param list<string> $fixtures
     */
    #[TestWith([JsonFraming::Array, ['five_rows.json', 'misfit.json', 'parity_people.json']])]
    #[TestWith([JsonFraming::Lines, ['five_rows.jsonl', 'misfit.jsonl', 'parity_people.jsonl']])]
    public function test_unbounded_inference_is_independent_of_execution_order(
        JsonFraming $format,
        array $fixtures,
    ): void {
        $inference = new SchemaInference(-1, -1, unionByName: true);

        static::assertEquals(
            (new SchemaInferrer($inference))->infer(JsonFixtureContext::sniffer(
                $format,
                JsonFixtureContext::sources(...$fixtures),
            ))->schema,
            ReversedSniffing::schema(
                JsonFixtureContext::sniffer($format, JsonFixtureContext::sources(...$fixtures)),
                $inference,
            ),
        );
    }

    public function test_a_unit_sniffer_declares_its_readers_json(): void
    {
        $unit = JsonFixtureContext::sniffer(JsonFraming::Lines, JsonFixtureContext::sources(
            'five_rows.jsonl',
            'blank_line.jsonl',
        ))->parts(1, 8 << 20)[0];

        static::assertInstanceOf(JsonSniffer::class, $unit);
        static::assertSame(['json'], $unit->describe()?->extensions());
    }
}
