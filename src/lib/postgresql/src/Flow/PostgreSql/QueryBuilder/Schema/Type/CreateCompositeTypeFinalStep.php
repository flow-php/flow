<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Type;

use Flow\PostgreSql\Protobuf\AST\CompositeTypeStmt;
use Flow\PostgreSql\QueryBuilder\SqlQuery;

interface CreateCompositeTypeFinalStep extends SqlQuery
{
    public function toAst() : CompositeTypeStmt;

    public function toSql() : string;
}
