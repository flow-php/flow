<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Schema;

use Flow\PostgreSql\Protobuf\AST\AlterOwnerStmt;
use Flow\PostgreSql\QueryBuilder\Sql;

interface AlterSchemaOwnerFinalStep extends Sql
{
    public function toAst() : AlterOwnerStmt;

    public function toSql() : string;
}
