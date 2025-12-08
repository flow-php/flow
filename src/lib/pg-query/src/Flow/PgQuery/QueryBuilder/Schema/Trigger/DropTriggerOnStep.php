<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Trigger;

interface DropTriggerOnStep
{
    public function ifExists() : self;

    public function on(string $table, ?string $schema = null) : DropTriggerFinalStep;
}
