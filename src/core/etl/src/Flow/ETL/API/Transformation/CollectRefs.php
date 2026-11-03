<?php

declare(strict_types=1);

namespace Flow\ETL\API\Transformation;

use Flow\ETL\API\DataFrame;
use Flow\ETL\API\Transformation;
use Flow\ETL\Functions\Reference\References;
use NoDiscard;

final readonly class CollectRefs implements Transformation
{
    public function __construct(
        private References $references,
    ) {}

    #[NoDiscard]
    public function __invoke(DataFrame $dataFrame): DataFrame
    {
        return $dataFrame->collectRefs($this->references);
    }
}
