<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

final class Table
{
    /**
     * @param string $name
     * @param null|array<string> $columns
     */
    public function __construct(
        public readonly string $name,
        public readonly ?array $columns = []
    ) {
    }
}
