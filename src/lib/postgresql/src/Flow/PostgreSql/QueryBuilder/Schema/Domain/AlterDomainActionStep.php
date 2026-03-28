<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Domain;

use Flow\PostgreSql\QueryBuilder\Condition\Condition;
use Flow\PostgreSql\QueryBuilder\Expression\Expression;

interface AlterDomainActionStep
{
    public function addConstraint(string $name, Condition $condition) : AlterDomainFinalStep;

    public function dropConstraint(string $name) : AlterDomainFinalStep;

    public function dropDefault() : AlterDomainFinalStep;

    public function dropNotNull() : AlterDomainFinalStep;

    public function setDefault(Expression $expression) : AlterDomainFinalStep;

    public function setNotNull() : AlterDomainFinalStep;

    public function validateConstraint(string $name) : AlterDomainFinalStep;
}
