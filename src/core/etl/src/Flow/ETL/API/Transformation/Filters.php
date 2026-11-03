<?php

declare(strict_types=1);

namespace Flow\ETL\API\Transformation;

use Flow\ETL\API\DataFrame;
use Flow\ETL\API\Transformation;
use Flow\ETL\Functions\ScalarFunction;
use NoDiscard;

final readonly class Filters implements Transformation
{
    /**
     * @param array<ScalarFunction> $functions
     */
    public function __construct(
        private array $functions,
    ) {}

    #[NoDiscard]
    public function __invoke(DataFrame $dataFrame): DataFrame
    {
        return $dataFrame->filters($this->functions);
    }
}
