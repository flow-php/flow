<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Ownership;

use Flow\PostgreSql\Protobuf\AST\ReassignOwnedStmt;

interface ReassignOwnedFinalStep
{
    public function toAst() : ReassignOwnedStmt;

    public function toSql() : string;
}
