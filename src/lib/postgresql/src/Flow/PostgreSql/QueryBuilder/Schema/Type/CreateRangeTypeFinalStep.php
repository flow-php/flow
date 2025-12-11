<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Type;

use Flow\PostgreSql\Protobuf\AST\CreateRangeStmt;

interface CreateRangeTypeFinalStep
{
    public function toAst() : CreateRangeStmt;

    public function toSql() : string;
}
