<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Domain;

use Flow\PostgreSql\QueryBuilder\Condition\Condition;
use Flow\PostgreSql\QueryBuilder\Expression\Expression;

interface CreateDomainOptionsStep extends CreateDomainFinalStep
{
    public function check(Condition $condition) : self;

    public function collate(string $collation) : self;

    public function constraint(string $name) : self;

    public function default(Expression $expression) : self;

    public function notNull() : self;

    public function null() : self;
}
