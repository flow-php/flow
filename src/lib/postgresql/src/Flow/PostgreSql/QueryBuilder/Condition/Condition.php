<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Condition;

use Flow\PostgreSql\QueryBuilder\Expression\Expression;

interface Condition extends Expression
{
    public function and(self $other): AndCondition;

    public function not(): NotCondition;

    public function or(self $other): OrCondition;
}
