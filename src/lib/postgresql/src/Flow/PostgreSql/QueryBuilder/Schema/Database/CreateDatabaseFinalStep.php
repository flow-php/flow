<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Database;

use Flow\PostgreSql\Protobuf\AST\CreatedbStmt;
use Flow\PostgreSql\QueryBuilder\Sql;

interface CreateDatabaseFinalStep extends Sql
{
    public function toAst() : CreatedbStmt;

    public function toSql() : string;
}
