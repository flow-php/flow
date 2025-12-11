<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Domain;

interface CreateDomainOptionsStep extends CreateDomainFinalStep
{
    public function check(string $expression) : self;

    public function collate(string $collation) : self;

    public function constraint(string $name) : self;

    public function default(string $expression) : self;

    public function notNull() : self;

    public function null() : self;
}
