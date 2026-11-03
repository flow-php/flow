<?php

declare(strict_types=1);

namespace Flow\ETL\API\Pipe;

use Flow\ETL\API\DataFrame;
use Flow\ETL\Values\Rows;

final readonly class Fetch
{
    public function __construct(
        private ?int $limit = null,
    ) {}

    public function __invoke(DataFrame $dataFrame): Rows
    {
        return $dataFrame->fetch($this->limit);
    }
}
