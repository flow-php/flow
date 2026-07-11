<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Sort;

use Flow\ETL\Sort\SortAlgorithms;

final readonly class SortConfig
{
    public function __construct(
        public SortAlgorithms $algorithm,
        public string $filesystemProtocol = 'file',
    ) {}
}
