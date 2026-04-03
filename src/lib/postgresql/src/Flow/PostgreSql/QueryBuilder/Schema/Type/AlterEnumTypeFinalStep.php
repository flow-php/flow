<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Type;

use Flow\PostgreSql\Protobuf\AST\AlterEnumStmt;
use Flow\PostgreSql\QueryBuilder\Sql;

interface AlterEnumTypeFinalStep extends Sql
{
    public function ifNotExists() : self;

    public function toAst() : AlterEnumStmt;

    public function toSql() : string;
}
