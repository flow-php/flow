<?php

declare(strict_types=1);

namespace Flow\ETL\API\Pipe;

use Flow\ETL\API\DataFrame;
use Flow\ETL\API\GroupedDataFrame;
use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\Planning\Logical\Node\BucketingAlgorithmBuilder;
use NoDiscard;

final readonly class GroupBy
{
    /**
     * @param list<Reference|string>|Reference|string $columns a single column is grouped by on its own
     * @param null|BucketingAlgorithmBuilder $algorithm null defers to configuration; a builder pins the
     *                                               algorithm for this operation and skips any automatic choice
     */
    public function __construct(
        private array|Reference|string $columns,
        private ?BucketingAlgorithmBuilder $algorithm = null,
    ) {}

    #[NoDiscard]
    public function __invoke(DataFrame $dataFrame): GroupedDataFrame
    {
        return $dataFrame->groupBy($this->columns, $this->algorithm);
    }
}
