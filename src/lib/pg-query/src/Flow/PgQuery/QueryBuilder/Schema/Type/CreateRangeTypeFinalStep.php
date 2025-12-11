<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Type;

use Flow\PgQuery\Protobuf\AST\CreateRangeStmt;

interface CreateRangeTypeFinalStep
{
    public function toAst() : CreateRangeStmt;

    public function toSql() : string;
}
