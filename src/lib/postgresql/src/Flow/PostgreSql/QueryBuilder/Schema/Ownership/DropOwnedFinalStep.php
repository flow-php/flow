<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Ownership;

use Flow\PostgreSql\Protobuf\AST\DropOwnedStmt;
use Flow\PostgreSql\QueryBuilder\SqlQuery;

interface DropOwnedFinalStep extends SqlQuery
{
    public function cascade() : self;

    public function restrict() : self;

    public function toAst() : DropOwnedStmt;

    public function toSql() : string;
}
