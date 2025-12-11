<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Grant;

interface GrantOnStep
{
    public function onAllTablesInSchema(string ...$schemas) : GrantToStep;

    public function onTable(string ...$tables) : GrantToStep;
}
