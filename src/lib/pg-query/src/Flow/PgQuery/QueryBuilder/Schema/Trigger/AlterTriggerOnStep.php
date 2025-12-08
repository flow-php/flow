<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Trigger;

interface AlterTriggerOnStep
{
    public function on(string $table, ?string $schema = null) : AlterTriggerActionStep;
}
