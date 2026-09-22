<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Integration;

use Flow\ETL\Adapter\CSV\CSVSampledRows;
use Flow\ETL\Adapter\CSV\Tests\Context\CSVFixtureContext;
use Flow\ETL\Schema\Inference\SchemaInference;
use Flow\ETL\Schema\Inference\TypeFloor;
use Flow\ETL\Tests\Double\CountingFilesystem;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Types\Type\Native\String\StringTypeNarrower;
use PHPUnit\Framework\Attributes\TestWith;

use function array_keys;
use function Flow\ETL\DSL\infer_schema;
use function Flow\Types\DSL\type_html;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function iterator_to_array;

final class CSVFileSampleTest extends FlowTestCase
{
    public function test_iterating_yields_every_record_of_its_source(): void
    {
        static::assertCount(5, iterator_to_array(CSVFixtureContext::sample('five_rows.csv'), false));
    }

    public function test_iterating_yields_every_row_with_the_full_key_set(): void
    {
        $records = iterator_to_array(CSVFixtureContext::sample('ragged.csv'), false);

        static::assertCount(3, $records);

        foreach ($records as $record) {
            static::assertSame(['id', 'name', 'v'], array_keys($record->values));
        }
    }

    public function test_an_abandoned_iteration_closes_its_stream(): void
    {
        $counting = new CountingFilesystem(new NativeLocalFilesystem());
        $iterator = CSVFixtureContext::sample('five_rows.csv', $counting)->getIterator();

        $iterator->current();
        unset($iterator);

        static::assertSame(1, $counting->closedStreams());
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

        [$observed, $sniffed] = CSVFixtureContext::sniffBothWays(
            'orders_flow.csv',
            $rowBudget,
            $inference,
            new StringTypeNarrower($inference->candidates()->toArray()),
        );

        static::assertSame($observed->rows(), $sniffed->rows());
        static::assertEquals(
            $observed->schema(new TypeFloor($inference->candidates())),
            $sniffed->schema(new TypeFloor($inference->candidates())),
        );
    }

    public function test_sniffing_narrows_with_the_narrower_it_is_handed(): void
    {
        $inference = infer_schema()->build();
        [$observed, $sniffed] = CSVFixtureContext::sniffBothWays(
            'orders_flow.csv',
            -1,
            $inference,
            new StringTypeNarrower([type_string()]),
        );

        static::assertEquals(
            $observed->schema(new TypeFloor($inference->candidates())),
            $sniffed->schema(new TypeFloor($inference->candidates())),
        );
    }

    public function test_nothing_is_sampled_until_a_sniff_ran(): void
    {
        static::assertNull(CSVFixtureContext::sample('five_rows.csv')->sampled());
    }

    public function test_a_sniff_that_reached_the_end_sampled_the_whole_file(): void
    {
        $sample = CSVFixtureContext::sample('five_rows.csv');

        $sample->sniffColumnTypes(['id', 'name'], 10, new SchemaInference(), new StringTypeNarrower());

        static::assertEquals(new CSVSampledRows(5, 20, true), $sample->sampled());
    }

    public function test_a_sniff_without_a_budget_sampled_the_whole_file(): void
    {
        $sample = CSVFixtureContext::sample('five_rows.csv');

        $sample->sniffColumnTypes(['id', 'name'], -1, new SchemaInference(), new StringTypeNarrower());

        static::assertEquals(new CSVSampledRows(5, 20, true), $sample->sampled());
    }

    public function test_a_sniff_stopped_by_its_budget_did_not_sample_the_whole_file(): void
    {
        $sample = CSVFixtureContext::sample('five_rows.csv');

        $sample->sniffColumnTypes(['id', 'name'], 5, new SchemaInference(), new StringTypeNarrower());

        static::assertEquals(new CSVSampledRows(5, 20, false), $sample->sampled());
    }
}
