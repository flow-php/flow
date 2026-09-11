<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Integration;

use Flow\ETL\Adapter\CSV\Tests\Context\CSVFixtureContext;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use PHPUnit\Framework\Attributes\TestWith;

use function array_keys;
use function count;
use function iterator_to_array;

final class CSVFileReaderTest extends FlowTestCase
{
    public function test_batches_are_sized_and_the_tail_is_shorter(): void
    {
        $sizes = [];

        foreach (CSVFixtureContext::reader()->batches(CSVFixtureContext::source('five_rows.csv'), 2) as $batch) {
            $sizes[] = count($batch);
        }

        static::assertSame([2, 2, 1], $sizes);
    }

    public function test_batches_closes_its_stream_when_abandoned(): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());
        $batches = CSVFixtureContext::reader($counting)->batches(CSVFixtureContext::source('five_rows.csv'), 2);

        $batches->current();
        unset($batches);

        static::assertSame(1, $counting->closedStreams());
    }

    #[TestWith(['empty.csv'])]
    #[TestWith(['header_only.csv'])]
    public function test_batches_never_yields_an_empty_batch(string $fixture): void
    {
        static::assertSame(
            [],
            iterator_to_array(CSVFixtureContext::reader()->batches(CSVFixtureContext::source($fixture), 10), false),
        );
    }

    public function test_columns_of_one_source(): void
    {
        static::assertSame(
            ['id', 'name'],
            CSVFixtureContext::reader()->columns(CSVFixtureContext::source('header_only.csv')),
        );
        static::assertSame([], CSVFixtureContext::reader()->columns(CSVFixtureContext::source('empty.csv')));
    }

    public function test_header_closes_every_stream_it_opens(): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());

        CSVFixtureContext::globReader('glob_with_empty/*.csv', $counting)->header();

        static::assertSame($counting->readFromCalls, $counting->closedStreams());
    }

    public function test_header_is_empty_when_no_source_has_one(): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());
        $header = CSVFixtureContext::globReader('two_empty/*.csv', $counting)->header();

        static::assertSame([], $header->names);
        static::assertNull($header->source);
        static::assertSame(2, $counting->readFromCalls);
    }

    public function test_header_is_empty_when_the_listing_is_empty(): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());
        $header = CSVFixtureContext::reader($counting)->header();

        static::assertSame([], $header->names);
        static::assertNull($header->source);
        static::assertSame(0, $counting->readFromCalls);
    }

    /**
     * @param non-empty-string $expectedSource
     */
    #[TestWith(['empty_then_header_only', 'b.csv'])]
    #[TestWith(['header_only_then_empty', 'a.csv'])]
    public function test_header_is_the_first_source_that_has_one(string $dir, string $expectedSource): void
    {
        $header = CSVFixtureContext::globReader($dir . '/*.csv')->header();

        static::assertSame(['id', 'name'], $header->names);
        static::assertNotNull($header->source);
        static::assertStringEndsWith($expectedSource, $header->source);
    }

    public function test_header_stops_at_the_first_source_with_a_header(): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());
        $header = CSVFixtureContext::globReader('columns_diverge/*.csv', $counting)->header();

        static::assertSame(1, $counting->readFromCalls);
        static::assertNotNull($header->source);
        static::assertStringEndsWith('a.csv', $header->source);
    }

    public function test_sample_closes_its_stream_when_abandoned(): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());
        $sample = CSVFixtureContext::reader($counting)->sample(CSVFixtureContext::source('five_rows.csv'));

        $sample->current();
        unset($sample);

        static::assertSame(1, $counting->closedStreams());
    }

    public function test_sample_yields_every_row_of_a_source(): void
    {
        $records = iterator_to_array(
            CSVFixtureContext::reader()->sample(CSVFixtureContext::source('ragged.csv')),
            false,
        );

        static::assertCount(3, $records);

        foreach ($records as $record) {
            static::assertSame(['id', 'name', 'v'], array_keys($record->values));
        }
    }

    public function test_samples_does_not_ration_the_row_budget(): void
    {
        $units = iterator_to_array(
            CSVFixtureContext::reader(sources: CSVFixtureContext::sources('five_rows.csv'))->samples(1),
            false,
        );

        static::assertCount(5, iterator_to_array($units[0], false));
    }

    public function test_samples_walks_the_sources_in_listing_order(): void
    {
        $first = [];

        foreach (CSVFixtureContext::globReader('columns_diverge/*.csv')->samples(10) as $unit) {
            foreach ($unit as $values) {
                $first[] = $values->values;

                break;
            }
        }

        static::assertSame([['id' => '1', 'name' => 'a'], ['id' => '3', 'label' => 'c']], $first);
    }

    public function test_samples_yields_one_unstarted_generator_per_source(): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());
        $units = iterator_to_array(
            CSVFixtureContext::globReader('columns_diverge/*.csv', $counting)->samples(10),
            false,
        );

        $beforeAdvancing = $counting->readFromCalls;

        $units[0]->current();

        $afterAdvancingOne = $counting->readFromCalls;

        static::assertCount(2, $units);
        static::assertSame(0, $beforeAdvancing, 'samples() yields UNSTARTED generators');
        static::assertSame(1, $afterAdvancingOne, 'advancing the first unit opens exactly one source');
    }
}
