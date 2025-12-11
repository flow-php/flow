<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Utility;

use Flow\PostgreSql\Protobuf\AST\DiscardStmt;

interface DiscardFinalStep
{
    public function toAst() : DiscardStmt;

    public function toSql() : string;
}
