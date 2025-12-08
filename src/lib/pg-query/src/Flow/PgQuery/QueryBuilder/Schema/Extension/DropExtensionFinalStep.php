<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Extension;

use Flow\PgQuery\Protobuf\AST\DropStmt;

interface DropExtensionFinalStep
{
    public function cascade() : self;

    public function ifExists() : self;

    public function restrict() : self;

    public function toAst() : DropStmt;
}
