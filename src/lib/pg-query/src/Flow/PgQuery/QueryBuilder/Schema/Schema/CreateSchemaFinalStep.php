<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Schema;

use Flow\PgQuery\Protobuf\AST\CreateSchemaStmt;

interface CreateSchemaFinalStep
{
    public function toAst() : CreateSchemaStmt;

    public function toSql() : string;
}
