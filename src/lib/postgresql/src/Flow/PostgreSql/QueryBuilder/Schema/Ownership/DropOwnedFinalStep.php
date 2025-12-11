<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Ownership;

use Flow\PostgreSql\Protobuf\AST\DropOwnedStmt;

interface DropOwnedFinalStep
{
    public function cascade() : self;

    public function restrict() : self;

    public function toAst() : DropOwnedStmt;

    public function toSql() : string;
}
