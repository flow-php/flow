<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Rule;

interface CreateRuleToStep
{
    public function to(string $table, ?string $schema = null) : CreateRuleWhereStep;
}
