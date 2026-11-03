<?php

declare(strict_types=1);

namespace Flow\ETL\API\Transformation;

use Flow\ETL\API\DataFrame;
use Flow\ETL\API\Transformation;
use NoDiscard;

final readonly class Offset implements Transformation
{
    /**
     * @param ?int<0, max> $offset
     */
    public function __construct(
        private ?int $offset,
    ) {}

    #[NoDiscard]
    public function __invoke(DataFrame $dataFrame): DataFrame
    {
        return $dataFrame->offset($this->offset);
    }
}
