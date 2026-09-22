<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Integration\JSONMachine;

use Flow\ETL\Adapter\JSON\JSONMachine\JsonFileSample;
use Flow\ETL\Adapter\JSON\JSONMachine\JsonFormat;
use Flow\ETL\Adapter\JSON\JSONMachine\JsonSampledFiles;
use Flow\ETL\Adapter\JSON\JSONMachine\JsonSamples;
use Flow\ETL\Adapter\JSON\Tests\Context\JsonFixtureContext;
use Flow\ETL\Tests\FlowTestCase;
use Generator;

use function iterator_to_array;

final class JsonSamplesTest extends FlowTestCase
{
    public function test_it_folds_what_the_pulled_samples_read(): void
    {
        $samples = new JsonSamples([
            new JsonFileSample(
                JsonFixtureContext::reader(JsonFormat::Lines),
                JsonFixtureContext::source('five_rows.jsonl'),
            ),
            new JsonFileSample(
                JsonFixtureContext::reader(JsonFormat::Lines),
                JsonFixtureContext::source('blank_line.jsonl'),
            ),
        ]);

        foreach ($samples as $sample) {
            iterator_to_array($sample, false);
        }

        static::assertEquals(new JsonSampledFiles(2, 7, 64, true), $samples->sampledFiles());
    }

    public function test_samples_past_the_last_pulled_one_are_never_created(): void
    {
        $created = 0;
        $samples = new JsonSamples(
            (
                /** @return Generator<int, JsonFileSample> */
                static function () use (&$created): Generator {
                    foreach (['five_rows.jsonl', 'five_rows.jsonl'] as $fixture) {
                        $created++;

                        yield new JsonFileSample(
                            JsonFixtureContext::reader(JsonFormat::Lines),
                            JsonFixtureContext::source($fixture),
                        );
                    }
                }
            )(),
        );

        foreach ($samples as $sample) {
            iterator_to_array($sample, false);

            break;
        }

        static::assertSame(1, $created);
        static::assertEquals(new JsonSampledFiles(1, 5, 45, true), $samples->sampledFiles());
    }

    public function test_a_pulled_sample_that_was_never_iterated_is_not_counted(): void
    {
        $samples = new JsonSamples([
            new JsonFileSample(
                JsonFixtureContext::reader(JsonFormat::Lines),
                JsonFixtureContext::source('five_rows.jsonl'),
            ),
        ]);

        iterator_to_array($samples, false);

        static::assertEquals(new JsonSampledFiles(0, 0, 0, true), $samples->sampledFiles());
    }
}
