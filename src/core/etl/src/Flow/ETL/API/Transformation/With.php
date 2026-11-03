<?php

declare(strict_types=1);

namespace Flow\ETL\API\Transformation;

use Flow\ETL\API\DataFrame;
use Flow\ETL\API\Transformation;
use Flow\ETL\Operators\Transformer;
use NoDiscard;

final readonly class With implements Transformation
{
    public function __construct(
        private Transformer|Transformation $transformer,
    ) {}

    #[NoDiscard]
    public function __invoke(DataFrame $dataFrame): DataFrame
    {
        return $dataFrame->with($this->transformer);
    }
}
