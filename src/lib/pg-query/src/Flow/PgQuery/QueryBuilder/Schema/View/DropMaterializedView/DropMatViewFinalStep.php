<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\View\DropMaterializedView;

use Flow\PgQuery\Protobuf\AST\DropStmt;

interface DropMatViewFinalStep
{
    public function cascade() : self;

    public function ifExists() : self;

    public function restrict() : self;

    public function toAst() : DropStmt;
}
