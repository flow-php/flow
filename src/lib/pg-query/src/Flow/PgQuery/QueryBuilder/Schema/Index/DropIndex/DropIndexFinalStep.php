<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Index\DropIndex;

use Flow\PgQuery\Protobuf\AST\DropStmt;

interface DropIndexFinalStep
{
    public function cascade() : self;

    public function concurrently() : self;

    public function ifExists() : self;

    public function restrict() : self;

    public function toAst() : DropStmt;
}
