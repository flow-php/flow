<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Schema;

use Flow\PostgreSql\Protobuf\AST\CreateSchemaStmt;
use Flow\PostgreSql\QueryBuilder\SqlQuery;

interface CreateSchemaFinalStep extends SqlQuery
{
    public function toAst() : CreateSchemaStmt;

    public function toSql() : string;
}
