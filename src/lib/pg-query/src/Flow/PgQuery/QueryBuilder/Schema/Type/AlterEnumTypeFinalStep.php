<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Type;

use Flow\PgQuery\Protobuf\AST\AlterEnumStmt;

interface AlterEnumTypeFinalStep
{
    public function ifNotExists() : self;

    public function toAst() : AlterEnumStmt;
}
