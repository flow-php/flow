<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Type;

use Flow\PostgreSql\Protobuf\AST\CreateRangeStmt;
use Flow\PostgreSql\QueryBuilder\Sql;

interface CreateRangeTypeFinalStep extends Sql
{
    public function toAst() : CreateRangeStmt;

    public function toSql() : string;
}
