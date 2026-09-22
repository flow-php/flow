<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Unit\JSONMachine;

use Flow\ETL\Adapter\JSON\JSONMachine\JsonSampledFiles;
use Flow\ETL\Cardinality;
use Flow\ETL\Tests\FlowTestCase;

final class JsonSampledFilesTest extends FlowTestCase
{
    public function test_a_partial_sample_estimates_from_the_mean_sampled_row(): void
    {
        static::assertEquals(
            Cardinality::approximately(100),
            (new JsonSampledFiles(1, 10, 100, false))->estimatedRows(1, Cardinality::exact(1000)),
        );
    }

    public function test_every_listed_file_read_whole_is_exact(): void
    {
        $sampled = new JsonSampledFiles(2, 10, 100, true);

        static::assertEquals(Cardinality::exact(10), $sampled->estimatedRows(2, Cardinality::exact(100)));
        static::assertEquals(Cardinality::exact(10), $sampled->exactRows(2));
    }

    public function test_a_listed_file_the_samples_never_read_is_not_exact(): void
    {
        $sampled = new JsonSampledFiles(1, 10, 100, true);

        static::assertEquals(Cardinality::approximately(20), $sampled->estimatedRows(2, Cardinality::exact(200)));
        static::assertEquals(Cardinality::unknown(), $sampled->exactRows(2));
    }

    public function test_a_partial_document_sample_is_unknown(): void
    {
        static::assertEquals(Cardinality::unknown(), (new JsonSampledFiles(1, 10, 0, false))->exactRows(1));
    }

    public function test_no_listed_bytes_or_no_sampled_bytes_is_unknown(): void
    {
        static::assertEquals(Cardinality::unknown(), (new JsonSampledFiles(1, 10, 100, false))->estimatedRows(
            1,
            Cardinality::unknown(),
        ));
        static::assertEquals(Cardinality::unknown(), (new JsonSampledFiles(1, 10, 0, false))->estimatedRows(
            1,
            Cardinality::exact(100),
        ));
    }
}
