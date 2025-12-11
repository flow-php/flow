<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Type;

use Flow\PgQuery\Protobuf\AST\CreateEnumStmt;

interface CreateEnumTypeFinalStep
{
    public function toAst() : CreateEnumStmt;

    public function toSql() : string;
}
