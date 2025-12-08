<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Grant;

interface RevokeOnStep
{
    public function onAllTablesInSchema(string ...$schemas) : RevokeFromStep;

    public function onTable(string ...$tables) : RevokeFromStep;
}
