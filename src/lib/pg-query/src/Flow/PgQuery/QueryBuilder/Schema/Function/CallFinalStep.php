<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Function;

use Flow\PgQuery\Protobuf\AST\CallStmt;

interface CallFinalStep
{
    public function toAst() : CallStmt;

    public function with(mixed ...$args) : self;
}
