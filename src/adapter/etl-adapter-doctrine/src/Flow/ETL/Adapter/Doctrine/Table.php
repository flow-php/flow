<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

final readonly class Table
{
    /**
     * @param string $name
     * @param null|array<string> $columns
     */
    public function __construct(
        public string $name,
        public ?array $columns = [],
    ) {
    }
}
