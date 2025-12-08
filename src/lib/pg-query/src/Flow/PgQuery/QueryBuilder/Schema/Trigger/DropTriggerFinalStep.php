<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Trigger;

use Flow\PgQuery\Protobuf\AST\DropStmt;

interface DropTriggerFinalStep
{
    public function cascade() : self;

    public function restrict() : self;

    public function toAst() : DropStmt;
}
