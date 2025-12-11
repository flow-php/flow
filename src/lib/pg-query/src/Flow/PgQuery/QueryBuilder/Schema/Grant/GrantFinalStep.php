<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Grant;

use Flow\PgQuery\Protobuf\AST\GrantStmt;

interface GrantFinalStep
{
    public function toAst() : GrantStmt;

    public function toSql() : string;

    public function withGrantOption() : self;
}
