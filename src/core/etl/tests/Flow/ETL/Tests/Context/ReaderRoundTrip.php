<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\Reader;
use Flow\ETL\Runtime\FlowContext;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Path;
use RuntimeException;

use function bin2hex;
use function Flow\Filesystem\DSL\path;
use function is_dir;
use function mkdir;
use function random_bytes;
use function sys_get_temp_dir;

final class ReaderRoundTrip
{
    /**
     * The batch sizes a reader yields for a scan, in order.
     *
     * @return list<int>
     */
    public static function batches(Reader $reader, FlowContext $context, Criteria $criteria = new Criteria()): array
    {
        $sizes = [];

        foreach (FlowTestCase::extracted($reader, $context->backend(), $criteria) as $rows) {
            $sizes[] = $rows->count();
        }

        return $sizes;
    }

    /**
     * The batch sizes an extractor's own read yields, in order.
     *
     * @return list<int>
     */
    public static function extractorBatches(Extractor $extractor, FlowContext $context): array
    {
        $sizes = [];

        foreach (FlowTestCase::extracted($extractor, $context->backend(), new Criteria()) as $rows) {
            $sizes[] = $rows->count();
        }

        return $sizes;
    }

    /**
     * Every row a reader yields for a scan, in order, as plain arrays.
     *
     * @return list<array<array-key, mixed>>
     */
    public static function rows(Reader $reader, FlowContext $context, Criteria $criteria = new Criteria()): array
    {
        $rows = [];

        foreach (FlowTestCase::extracted($reader, $context->backend(), $criteria) as $batch) {
            foreach ($batch->toArray() as $row) {
                $rows[] = $row;
            }
        }

        return $rows;
    }

    /**
     * A fresh directory on the local filesystem - the only one a rebuilt reader can open.
     */
    public static function scratch(string $name): Path
    {
        $directory = sys_get_temp_dir() . '/flow-reader-' . $name . '-' . bin2hex(random_bytes(4));

        if (!mkdir($directory, 0777, true) && !is_dir($directory)) {
            throw new RuntimeException('Failed to create ' . $directory);
        }

        return path('file://' . $directory);
    }
}
