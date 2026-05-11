<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Rule;

use Flow\PostgreSql\QueryBuilder\Condition\Condition;

interface CreateRuleWhereStep extends CreateRuleDoStep
{
    public function where(Condition $condition): CreateRuleDoStep;
}
