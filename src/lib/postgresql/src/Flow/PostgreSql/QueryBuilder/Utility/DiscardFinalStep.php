<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Utility;

use Flow\PostgreSql\Protobuf\AST\DiscardStmt;
use Flow\PostgreSql\QueryBuilder\Sql;

interface DiscardFinalStep extends Sql
{
    public function toAst(): DiscardStmt;

    public function toSql(): string;
}
