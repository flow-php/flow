<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Integration;

use Flow\ETL\Adapter\CSV\CSVFileSample;
use Flow\ETL\Adapter\CSV\CSVSampledFiles;
use Flow\ETL\Adapter\CSV\CSVSamples;
use Flow\ETL\Adapter\CSV\Tests\Context\CSVFixtureContext;
use Flow\ETL\Schema\Inference\SchemaInference;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Type\Native\String\StringTypeNarrower;
use Generator;

use function iterator_to_array;

final class CSVSamplesTest extends FlowTestCase
{
    public function test_it_folds_what_the_pulled_samples_read(): void
    {
        $samples = new CSVSamples([
            CSVFixtureContext::sample('five_rows.csv'),
            CSVFixtureContext::sample('five_rows.csv'),
        ]);

        foreach ($samples as $sample) {
            $sample->sniffColumnTypes(['id', 'name'], -1, new SchemaInference(), new StringTypeNarrower());
        }

        static::assertEquals(new CSVSampledFiles(2, 10, 40, true), $samples->sampledFiles());
    }

    public function test_samples_past_the_last_pulled_one_are_never_created(): void
    {
        $created = 0;
        $samples = new CSVSamples(
            (
                /** @return Generator<int, CSVFileSample> */
                static function () use (&$created): Generator {
                    foreach (['five_rows.csv', 'five_rows.csv', 'five_rows.csv'] as $fixture) {
                        $created++;

                        yield CSVFixtureContext::sample($fixture);
                    }
                }
            )(),
        );

        foreach ($samples as $sample) {
            $sample->sniffColumnTypes(['id', 'name'], -1, new SchemaInference(), new StringTypeNarrower());

            break;
        }

        static::assertSame(1, $created);
        static::assertEquals(new CSVSampledFiles(1, 5, 20, true), $samples->sampledFiles());
    }

    public function test_a_pulled_sample_that_was_never_sniffed_is_not_counted(): void
    {
        $samples = new CSVSamples([CSVFixtureContext::sample('five_rows.csv')]);

        iterator_to_array($samples, false);

        static::assertEquals(new CSVSampledFiles(0, 0, 0, true), $samples->sampledFiles());
    }
}
