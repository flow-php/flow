<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Grant;

use Flow\PostgreSql\Protobuf\AST\GrantStmt;
use Flow\PostgreSql\QueryBuilder\SqlQuery;

interface GrantFinalStep extends SqlQuery
{
    public function toAst() : GrantStmt;

    public function toSql() : string;

    public function withGrantOption() : self;
}
