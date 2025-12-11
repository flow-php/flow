<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Type;

use Flow\PostgreSql\Protobuf\AST\AlterEnumStmt;

interface AlterEnumTypeFinalStep
{
    public function ifNotExists() : self;

    public function toAst() : AlterEnumStmt;

    public function toSql() : string;
}
