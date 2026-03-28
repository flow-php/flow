<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Diff;

use Flow\PostgreSql\QueryBuilder\SqlQuery;
use Flow\PostgreSql\Schema\Procedure;

final readonly class ProcedureDiff implements Diff
{
    public function __construct(
        public Procedure $source,
        public Procedure $target,
    ) {
    }

    /**
     * @return list<SqlQuery>
     */
    public function generate() : array
    {
        $sql = $this->target->toSql();

        if ($sql === null) {
            return [];
        }

        return [$sql];
    }
}
