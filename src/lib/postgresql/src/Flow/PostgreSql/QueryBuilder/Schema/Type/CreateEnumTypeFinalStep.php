<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Type;

use Flow\PostgreSql\Protobuf\AST\CreateEnumStmt;
use Flow\PostgreSql\QueryBuilder\Sql;

interface CreateEnumTypeFinalStep extends Sql
{
    public function toAst() : CreateEnumStmt;

    public function toSql() : string;
}
