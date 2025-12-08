<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Trigger;

interface CreateTriggerOnStep
{
    public function on(string $table, ?string $schema = null) : CreateTriggerOptionsStep;
}
