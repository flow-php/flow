<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Trigger;

use Flow\PostgreSql\Protobuf\AST\DropStmt;
use Flow\PostgreSql\QueryBuilder\Sql;

interface DropTriggerFinalStep extends Sql
{
    public function cascade() : self;

    public function restrict() : self;

    public function toAst() : DropStmt;

    public function toSql() : string;
}
