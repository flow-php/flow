<?php

declare(strict_types=1);

namespace Flow\ETL\API\Transformation;

use Flow\ETL\API\DataFrame;
use Flow\ETL\API\Transformation;
use Flow\ETL\Functions\ScalarFunction;
use NoDiscard;

final readonly class Until implements Transformation
{
    public function __construct(
        private ScalarFunction $function,
    ) {}

    #[NoDiscard]
    public function __invoke(DataFrame $dataFrame): DataFrame
    {
        return $dataFrame->until($this->function);
    }
}
