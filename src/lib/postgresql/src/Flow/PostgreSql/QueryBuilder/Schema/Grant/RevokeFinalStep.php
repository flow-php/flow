<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Grant;

use Flow\PostgreSql\Protobuf\AST\GrantStmt;

interface RevokeFinalStep
{
    public function cascade() : self;

    public function restrict() : self;

    public function toAst() : GrantStmt;

    public function toSql() : string;
}
