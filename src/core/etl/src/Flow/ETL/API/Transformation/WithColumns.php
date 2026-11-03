<?php

declare(strict_types=1);

namespace Flow\ETL\API\Transformation;

use Flow\ETL\API\DataFrame;
use Flow\ETL\API\Transformation;
use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\Functions\WindowFunction;
use NoDiscard;

final readonly class WithColumns implements Transformation
{
    /**
     * @param array<int, WithColumn>|array<string, ScalarFunction|WindowFunction|WithColumn> $references
     */
    public function __construct(
        private array $references,
    ) {}

    #[NoDiscard]
    public function __invoke(DataFrame $dataFrame): DataFrame
    {
        return $dataFrame->withColumns($this->references);
    }
}
