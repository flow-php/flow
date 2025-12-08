<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Rule;

interface DropRuleOnStep
{
    public function ifExists() : self;

    public function on(string $table, ?string $schema = null) : DropRuleFinalStep;
}
