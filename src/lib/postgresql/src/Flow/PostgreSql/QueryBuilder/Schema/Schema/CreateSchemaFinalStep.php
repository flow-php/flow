<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Schema;

use Flow\PostgreSql\Protobuf\AST\CreateSchemaStmt;
use Flow\PostgreSql\QueryBuilder\Sql;

interface CreateSchemaFinalStep extends Sql
{
    public function toAst(): CreateSchemaStmt;

    public function toSql(): string;
}
