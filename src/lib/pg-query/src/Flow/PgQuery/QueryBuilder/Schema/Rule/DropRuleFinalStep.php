<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Rule;

use Flow\PgQuery\Protobuf\AST\DropStmt;

interface DropRuleFinalStep
{
    public function cascade() : self;

    public function restrict() : self;

    public function toAst() : DropStmt;

    public function toSql() : string;
}
