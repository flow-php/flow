<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Diff;

use Flow\PostgreSql\QueryBuilder\SqlQuery;

interface Diff
{
    /**
     * @return list<SqlQuery>
     */
    public function generate() : array;
}
