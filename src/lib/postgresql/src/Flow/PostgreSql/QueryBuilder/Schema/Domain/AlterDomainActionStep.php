<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Domain;

interface AlterDomainActionStep
{
    public function addConstraint(string $name, string $expression) : AlterDomainFinalStep;

    public function dropConstraint(string $name) : AlterDomainFinalStep;

    public function dropDefault() : AlterDomainFinalStep;

    public function dropNotNull() : AlterDomainFinalStep;

    public function setDefault(string $expression) : AlterDomainFinalStep;

    public function setNotNull() : AlterDomainFinalStep;

    public function validateConstraint(string $name) : AlterDomainFinalStep;
}
