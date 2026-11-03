<?php

declare(strict_types=1);

namespace Flow\ETL\API\Transformation;

use Flow\ETL\API\DataFrame;
use Flow\ETL\API\Transformation;
use NoDiscard;

final readonly class Rename implements Transformation
{
    public function __construct(
        private string $from,
        private string $to,
    ) {}

    #[NoDiscard]
    public function __invoke(DataFrame $dataFrame): DataFrame
    {
        return $dataFrame->rename($this->from, $this->to);
    }
}
