<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Rule;

use Flow\PostgreSql\Protobuf\AST\DropStmt;

interface DropRuleFinalStep
{
    public function cascade() : self;

    public function restrict() : self;

    public function toAst() : DropStmt;

    public function toSql() : string;
}
