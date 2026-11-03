<?php

declare(strict_types=1);

namespace Flow\ETL\API\Transformation;

use Flow\ETL\API\DataFrame;
use Flow\ETL\API\Transformation;
use NoDiscard;

final readonly class Limit implements Transformation
{
    public function __construct(
        private ?int $limit,
    ) {}

    #[NoDiscard]
    public function __invoke(DataFrame $dataFrame): DataFrame
    {
        return $dataFrame->limit($this->limit);
    }
}
