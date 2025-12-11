<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Function;

use Flow\PostgreSql\Protobuf\AST\CallStmt;

interface CallFinalStep
{
    public function toAst() : CallStmt;

    public function toSql() : string;

    public function with(mixed ...$args) : self;
}
