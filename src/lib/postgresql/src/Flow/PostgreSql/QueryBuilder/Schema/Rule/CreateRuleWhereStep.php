<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Rule;

interface CreateRuleWhereStep extends CreateRuleDoStep
{
    public function where(string $condition) : CreateRuleDoStep;
}
