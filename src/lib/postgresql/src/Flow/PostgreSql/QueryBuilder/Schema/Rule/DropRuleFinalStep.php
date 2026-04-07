<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Rule;

use Flow\PostgreSql\Protobuf\AST\DropStmt;
use Flow\PostgreSql\QueryBuilder\Sql;

interface DropRuleFinalStep extends Sql
{
    public function cascade() : self;

    public function restrict() : self;

    public function toAst() : DropStmt;

    public function toSql() : string;
}
