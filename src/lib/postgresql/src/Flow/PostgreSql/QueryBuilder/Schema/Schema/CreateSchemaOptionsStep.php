<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Schema;

interface CreateSchemaOptionsStep extends CreateSchemaFinalStep
{
    public function authorization(string $role) : CreateSchemaFinalStep;

    public function ifNotExists() : self;
}
