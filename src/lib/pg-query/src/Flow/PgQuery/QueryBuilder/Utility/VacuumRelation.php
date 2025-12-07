<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Utility;

final readonly class VacuumRelation
{
    /**
     * @param array<string> $columns
     */
    public function __construct(
        public string $table,
        public array $columns = [],
    ) {
    }
}
