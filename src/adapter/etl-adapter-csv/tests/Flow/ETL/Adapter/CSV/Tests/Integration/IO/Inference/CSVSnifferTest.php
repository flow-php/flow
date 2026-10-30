<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Integration\IO\Inference;

use Flow\ETL\Adapter\CSV\IO\Extractor\CSVReadOptions;
use Flow\ETL\Adapter\CSV\IO\Extractor\CSVSourceOpener;
use Flow\ETL\Adapter\CSV\IO\Inference\CSVSniffer;
use Flow\ETL\Adapter\CSV\Tests\Context\CSVFixtureContext;
use Flow\ETL\IO\Extractor\File\SourceFile;
use Flow\ETL\IO\Inference\InferredTypes;
use Flow\ETL\IO\Inference\SchemaInference;
use Flow\ETL\IO\Inference\SchemaInferrer;
use Flow\ETL\IO\Inference\TypeFloor;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\Context\ReversedSniffing;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use PHPUnit\Framework\Attributes\TestWith;

use function array_keys;
use function count;
use function Flow\ETL\DSL\infer_schema;
use function Flow\Types\DSL\type_html;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function strlen;

final class CSVSnifferTest extends FlowTestCase
{
    public function test_every_listed_file_is_one_unit(): void
    {
        static::assertSame(2, CSVFixtureContext::sniffer(['five_rows.csv', 'ragged.csv'])->units());
    }

    public function test_the_names_are_the_header_of_the_first_listed_file(): void
    {
        static::assertSame(['id', 'name'], CSVFixtureContext::sniffer(['five_rows.csv', 'ragged.csv'])->names());
    }

    public function test_no_file_is_opened_before_a_sniff(): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());

        CSVFixtureContext::sniffer(['five_rows.csv', 'ragged.csv'], filesystem: $counting)->units();

        static::assertSame(0, $counting->readFromCalls);
    }

    public function test_a_sniff_reads_every_record_of_its_unit(): void
    {
        static::assertSame(5, CSVFixtureContext::sniffer(['five_rows.csv'])->sniff(0, -1)->columns->rows);
    }

    public function test_a_ragged_unit_types_every_column_of_its_widest_row(): void
    {
        static::assertSame(
            ['id', 'name', 'v'],
            array_keys(
                CSVFixtureContext::sniffer(['ragged.csv'])
                    ->sniff(0, -1)
                    ->columns
                    ->schema(new TypeFloor(infer_schema()->build()->candidates()))
                    ->definitions(),
            ),
        );
    }

    public function test_a_sniff_stopped_by_its_budget_closes_every_stream_it_opened(): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());

        CSVFixtureContext::sniffer(['five_rows.csv'], filesystem: $counting)->sniff(0, 2);

        static::assertGreaterThan(0, $counting->readFromCalls);
        static::assertSame($counting->readFromCalls, $counting->closedStreams());
    }

    /**
     * @param int<0, max>|-1 $rowBudget
     */
    #[TestWith([-1, false])]
    #[TestWith([2, false])]
    #[TestWith([100, false])]
    #[TestWith([-1, true])]
    public function test_sniffing_equals_observing_its_records(int $rowBudget, bool $htmlCandidate): void
    {
        $inference = $htmlCandidate
            ? infer_schema()->types(type_html(), type_integer())->build()
            : infer_schema()->build();

        [$observed, $sniffed] = CSVFixtureContext::sniffBothWays('orders_flow.csv', $rowBudget, $inference);

        static::assertSame($observed->rows, $sniffed->rows);
        static::assertEquals(
            $observed->schema(new TypeFloor($inference->candidates())),
            $sniffed->schema(new TypeFloor($inference->candidates())),
        );
    }

    public function test_sniffing_narrows_with_the_candidates_it_is_handed(): void
    {
        $inference = infer_schema()->types(type_string())->build();
        [$observed, $sniffed] = CSVFixtureContext::sniffBothWays('orders_flow.csv', -1, $inference);

        static::assertEquals(
            $observed->schema(new TypeFloor($inference->candidates())),
            $sniffed->schema(new TypeFloor($inference->candidates())),
        );
    }

    public function test_a_sniff_that_reached_the_end_sampled_the_whole_file(): void
    {
        $sample = CSVFixtureContext::sniffer(['five_rows.csv'])->sniff(0, 10);

        static::assertSame([5, 20, true], [$sample->columns->rows, $sample->bytes, $sample->complete]);
    }

    public function test_a_sniff_without_a_budget_sampled_the_whole_file(): void
    {
        $sample = CSVFixtureContext::sniffer(['five_rows.csv'])->sniff(0, -1);

        static::assertSame([5, 20, true], [$sample->columns->rows, $sample->bytes, $sample->complete]);
    }

    public function test_a_sniff_stopped_by_its_budget_did_not_sample_the_whole_file(): void
    {
        $sample = CSVFixtureContext::sniffer(['five_rows.csv'])->sniff(0, 5);

        static::assertSame([5, 20, false], [$sample->columns->rows, $sample->bytes, $sample->complete]);
    }

    public function test_a_unit_sniffer_sniffs_the_same_as_the_parent(): void
    {
        $sniffer = CSVFixtureContext::sniffer(['five_rows.csv', 'ragged.csv']);
        $unit = $sniffer->parts(1, 8 << 20)[0];

        static::assertInstanceOf(CSVSniffer::class, $unit);
        static::assertSame(1, $unit->units());
        static::assertSame($sniffer->names(), $unit->names());
        static::assertEquals($sniffer->sniff(1, -1), $unit->sniff(0, -1));
    }

    public function test_a_unit_sniffer_rebuilt_from_its_description_sniffs_the_same(): void
    {
        $sniffer = CSVFixtureContext::sniffer(['five_rows.csv', 'ragged.csv']);
        $unit = $sniffer->parts(1, 8 << 20)[0];

        static::assertInstanceOf(CSVSniffer::class, $unit);

        $description = $unit->describe();

        static::assertNotNull($description);
        static::assertTrue($description->sensitive());
        static::assertEquals($sniffer->sniff(1, -1), DescribeRoundTrip::rebuilt($unit)->sniff(0, -1));
    }

    public function test_a_source_that_fits_the_target_or_has_no_size_is_one_part(): void
    {
        static::assertCount(1, CSVFixtureContext::sniffer(['orders_flow.csv'])->parts(0, 1024));
        static::assertCount(1, CSVFixtureContext::globSniffer('orders_flow.csv')->parts(0, 8 << 20));
    }

    public function test_a_large_source_is_cut_into_window_parts_whose_samples_merge_to_the_whole_sniff(): void
    {
        $sniffer = CSVFixtureContext::globSniffer('orders_flow.csv');
        $parts = $sniffer->parts(0, 1 << 20);

        static::assertCount(5, $parts);

        $merged = null;
        $bytes = 0;

        foreach ($parts as $part) {
            static::assertSame(1, $part->units());
            static::assertSame($sniffer->names(), $part->names());
            $bytes += $part->bytes(0);
            $sample = $part->sniff(0, -1);
            $merged = $merged === null ? $sample : $merged->merge($sample);
        }

        static::assertSame($sniffer->bytes(0), $bytes);
        static::assertEquals($sniffer->sniff(0, -1), $merged);
    }

    public function test_window_parts_resync_quoted_multiline_records(): void
    {
        $content = CSVFixtureContext::quotedMultilineRows(400);
        $sniffer = new CSVSniffer(
            new CSVSourceOpener(CSVFixtureContext::memory($content), new CSVReadOptions()),
            [new SourceFile(CSVFixtureContext::memorySource()->path, strlen($content))],
            InferredTypes::default(),
        );
        $parts = $sniffer->parts(0, 1024);

        static::assertGreaterThan(3, count($parts));

        $merged = null;

        foreach ($parts as $part) {
            $sample = $part->sniff(0, -1);
            $merged = $merged === null ? $sample : $merged->merge($sample);
        }

        static::assertNotNull($merged);
        static::assertSame(400, $merged->columns->rows);
        static::assertEquals($sniffer->sniff(0, -1), $merged);
    }

    public function test_a_window_part_rebuilt_from_its_description_sniffs_the_same(): void
    {
        $part = CSVFixtureContext::globSniffer('orders_flow.csv')->parts(0, 1 << 20)[2];

        static::assertInstanceOf(CSVSniffer::class, $part);

        static::assertNotNull($part->describe());
        static::assertEquals($part->sniff(0, -1), DescribeRoundTrip::rebuilt($part)->sniff(0, -1));
    }

    public function test_a_sniffer_over_a_memory_filesystem_does_not_describe(): void
    {
        static::assertNull(
            (new CSVSniffer(
                new CSVSourceOpener(CSVFixtureContext::memory("id,name\n1,a\n"), new CSVReadOptions()),
                [CSVFixtureContext::memorySource()],
                InferredTypes::default(),
            ))->describe(),
        );
    }

    public function test_bytes_are_the_listed_size(): void
    {
        static::assertSame(0, CSVFixtureContext::sniffer(['five_rows.csv'])->bytes(0));
    }

    public function test_unbounded_inference_is_independent_of_execution_order(): void
    {
        $inference = new SchemaInference(-1, -1, unionByName: true);
        $fixtures = ['five_rows.csv', 'ragged.csv', 'orders_flow.csv'];

        static::assertEquals(
            (new SchemaInferrer($inference))->infer(CSVFixtureContext::sniffer($fixtures, $inference))->schema,
            ReversedSniffing::schema(CSVFixtureContext::sniffer($fixtures, $inference), $inference),
        );
    }

    public function test_the_header_closes_every_stream_it_opens(): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());

        CSVFixtureContext::globSniffer('glob_with_empty/*.csv', filesystem: $counting)->header();

        static::assertSame($counting->readFromCalls, $counting->closedStreams());
    }

    public function test_the_header_is_empty_when_no_source_has_one(): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());
        $header = CSVFixtureContext::globSniffer('two_empty/*.csv', filesystem: $counting)->header();

        static::assertSame([], $header->names);
        static::assertNull($header->source);
        static::assertSame(2, $counting->readFromCalls);
    }

    public function test_the_header_is_empty_when_there_is_no_source(): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());
        $header = CSVFixtureContext::sniffer([], filesystem: $counting)->header();

        static::assertSame([], $header->names);
        static::assertNull($header->source);
        static::assertSame(0, $counting->readFromCalls);
    }

    /**
     * @param non-empty-string $expectedSource
     */
    #[TestWith(['empty_then_header_only', 'b.csv'])]
    #[TestWith(['header_only_then_empty', 'a.csv'])]
    public function test_the_header_is_the_first_source_that_has_one(string $dir, string $expectedSource): void
    {
        $header = CSVFixtureContext::globSniffer($dir . '/*.csv')->header();

        static::assertSame(['id', 'name'], $header->names);
        static::assertNotNull($header->source);
        static::assertStringEndsWith($expectedSource, $header->source);
    }

    public function test_the_header_stops_at_the_first_source_with_one(): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());
        $header = CSVFixtureContext::globSniffer('columns_diverge/*.csv', filesystem: $counting)->header();

        static::assertSame(1, $counting->readFromCalls);
        static::assertNotNull($header->source);
        static::assertStringEndsWith('a.csv', $header->source);
    }
}
