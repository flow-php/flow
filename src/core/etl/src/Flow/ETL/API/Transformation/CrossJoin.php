<?php

declare(strict_types=1);

namespace Flow\ETL\API\Transformation;

use Flow\ETL\API\DataFrame;
use Flow\ETL\API\Transformation;
use NoDiscard;

final readonly class CrossJoin implements Transformation
{
    public function __construct(
        private DataFrame $dataFrame,
        private string $prefix = '',
    ) {}

    #[NoDiscard]
    public function __invoke(DataFrame $dataFrame): DataFrame
    {
        return $dataFrame->crossJoin($this->dataFrame, $this->prefix);
    }
}
