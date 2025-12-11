<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Schema;

use Flow\PostgreSql\Protobuf\AST\CreateSchemaStmt;

interface CreateSchemaFinalStep
{
    public function toAst() : CreateSchemaStmt;

    public function toSql() : string;
}
