<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\View\DropView;

use Flow\PostgreSql\Protobuf\AST\DropStmt;

interface DropViewFinalStep
{
    public function cascade() : self;

    public function ifExists() : self;

    public function restrict() : self;

    public function toAst() : DropStmt;

    public function toSql() : string;
}
