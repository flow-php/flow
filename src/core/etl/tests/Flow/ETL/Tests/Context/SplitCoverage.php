<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\File\FileExtractor;
use Flow\ETL\IO\Extractor\File\FileSplit;
use Flow\ETL\IO\Extractor\SplittableExtractor;
use Flow\ETL\Runtime\FlowContext;
use Flow\ETL\Values\Rows;

use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\Types\DSL\type_instance_of;

final class SplitCoverage
{
    /**
     * Drains every split in list order and returns the rows, so a test can assert they equal a whole-source read
     * exactly - same rows, same order, no duplicates, no gaps.
     *
     * @param int<1, max> $targetSplitBytes
     * @param Criteria $criteria the criteria the splits are cut from and read under
     */
    public static function rowsFromSplits(
        SplittableExtractor&FileExtractor $extractor,
        FlowContext $context,
        int $targetSplitBytes,
        Criteria $criteria = new Criteria(),
    ): Rows {
        $extracted = null;

        foreach ($extractor->splits($criteria, $targetSplitBytes) as $split) {
            foreach ($extractor->extract(
                $context->backend(),
                $criteria->withSplit(type_instance_of(FileSplit::class)->assert($split)),
            ) as $batch) {
                $extracted = $extracted === null
                    ? $batch
                    : Rows::concat($extracted->schema(), $context->backend(), $extracted, $batch);
            }
        }

        return $extracted ?? rows(schema());
    }

    /**
     * The same, as a per-split row count, so a test can assert no split is empty-by-accident.
     *
     * @param int<1, max> $targetSplitBytes
     * @param Criteria $criteria the criteria the splits are cut from and read under
     *
     * @return list<int>
     */
    public static function rowCountPerSplit(
        SplittableExtractor&FileExtractor $extractor,
        FlowContext $context,
        int $targetSplitBytes,
        Criteria $criteria = new Criteria(),
    ): array {
        $counts = [];

        foreach ($extractor->splits($criteria, $targetSplitBytes) as $split) {
            $count = 0;

            foreach ($extractor->extract(
                $context->backend(),
                $criteria->withSplit(type_instance_of(FileSplit::class)->assert($split)),
            ) as $batch) {
                $count += $batch->count();
            }

            $counts[] = $count;
        }

        return $counts;
    }
}
