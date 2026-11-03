<?php

declare(strict_types=1);

namespace Flow\ETL\API\Transformation;

use Flow\ETL\API\DataFrame;
use Flow\ETL\API\Transformation;
use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\Planning\Logical\Node\SortAlgorithmBuilder;
use Flow\ETL\Values\Sort\SortColumn;
use NoDiscard;

final readonly class SortBy implements Transformation
{
    /**
     * @param list<Reference|SortColumn|string>|Reference|SortColumn|string $columns a single column is sorted by on its own; a reference sorts ascending
     * @param null|SortAlgorithmBuilder $algorithm null defers to configuration; a builder pins the algorithm
     */
    public function __construct(
        private array|Reference|SortColumn|string $columns,
        private ?SortAlgorithmBuilder $algorithm = null,
    ) {}

    #[NoDiscard]
    public function __invoke(DataFrame $dataFrame): DataFrame
    {
        return $dataFrame->sortBy($this->columns, $this->algorithm);
    }
}
