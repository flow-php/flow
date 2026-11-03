<?php

declare(strict_types=1);

namespace Flow\ETL\API\Transformation;

use Flow\ETL\API\DataFrame;
use Flow\ETL\API\GroupedDataFrame;
use Flow\ETL\API\Transformation;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\AggregatingFunction;
use Flow\ETL\Planning\Logical\Node\BucketingAlgorithmBuilder;
use NoDiscard;

final readonly class Aggregate implements Transformation
{
    /**
     * @param list<AggregatingFunction> $aggregations
     * @param null|BucketingAlgorithmBuilder $algorithm null defers to configuration; a builder pins the
     *                                               algorithm for this operation and skips any automatic choice
     */
    public function __construct(
        private array $aggregations,
        private ?BucketingAlgorithmBuilder $algorithm = null,
    ) {}

    /**
     * @throws InvalidArgumentException
     */
    #[NoDiscard]
    public function __invoke(DataFrame|GroupedDataFrame $dataFrame): DataFrame
    {
        if ($dataFrame instanceof DataFrame) {
            return $dataFrame->aggregate($this->aggregations, $this->algorithm);
        }

        if ($this->algorithm !== null) {
            throw InvalidArgumentException::because(
                'aggregate() after group_by() takes no bucketing algorithm. Pass it to group_by() instead.',
            );
        }

        return $dataFrame->aggregate(...$this->aggregations);
    }
}
