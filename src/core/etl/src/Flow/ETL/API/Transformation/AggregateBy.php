<?php

declare(strict_types=1);

namespace Flow\ETL\API\Transformation;

use Flow\ETL\API\DataFrame;
use Flow\ETL\API\Transformation;
use Flow\ETL\Operators\GroupBy;
use Flow\ETL\Planning\Logical\Node\BucketingAlgorithmBuilder;
use NoDiscard;

final readonly class AggregateBy implements Transformation
{
    /**
     * @param null|BucketingAlgorithmBuilder $algorithm null defers to configuration; a builder pins the
     *                                               algorithm for this operation and skips any automatic choice
     */
    public function __construct(
        private GroupBy $groupBy,
        private ?BucketingAlgorithmBuilder $algorithm = null,
    ) {}

    #[NoDiscard]
    public function __invoke(DataFrame $dataFrame): DataFrame
    {
        return $dataFrame->aggregateBy($this->groupBy, $this->algorithm);
    }
}
