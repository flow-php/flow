<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Utility;

use Flow\PgQuery\Protobuf\AST\DiscardStmt;

interface DiscardFinalStep
{
    public function toAst() : DiscardStmt;
}
