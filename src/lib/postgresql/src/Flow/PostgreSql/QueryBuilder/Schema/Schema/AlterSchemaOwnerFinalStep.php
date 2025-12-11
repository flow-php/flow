<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Schema;

use Flow\PostgreSql\Protobuf\AST\AlterOwnerStmt;

interface AlterSchemaOwnerFinalStep
{
    public function toAst() : AlterOwnerStmt;

    public function toSql() : string;
}
