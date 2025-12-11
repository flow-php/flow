<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Type;

use Flow\PostgreSql\Protobuf\AST\CreateEnumStmt;

interface CreateEnumTypeFinalStep
{
    public function toAst() : CreateEnumStmt;

    public function toSql() : string;
}
