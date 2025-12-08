<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Ownership;

use Flow\PgQuery\Protobuf\AST\ReassignOwnedStmt;

interface ReassignOwnedFinalStep
{
    public function toAst() : ReassignOwnedStmt;
}
