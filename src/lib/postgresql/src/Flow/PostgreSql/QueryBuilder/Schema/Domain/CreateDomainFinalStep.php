<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Domain;

use Flow\PostgreSql\Protobuf\AST\CreateDomainStmt;
use Flow\PostgreSql\QueryBuilder\Sql;

interface CreateDomainFinalStep extends Sql
{
    public function toAst(): CreateDomainStmt;

    public function toSql(): string;
}
