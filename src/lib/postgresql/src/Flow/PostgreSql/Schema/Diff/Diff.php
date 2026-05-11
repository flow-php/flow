<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Diff;

use Flow\PostgreSql\QueryBuilder\Sql;

interface Diff
{
    /**
     * @return list<Sql>
     */
    public function generate(): array;
}
