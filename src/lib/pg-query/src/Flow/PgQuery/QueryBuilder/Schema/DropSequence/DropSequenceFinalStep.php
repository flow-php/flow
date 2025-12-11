<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\DropSequence;

use Flow\PgQuery\Protobuf\AST\DropStmt;

interface DropSequenceFinalStep
{
    public function cascade() : self;

    public function ifExists() : self;

    public function restrict() : self;

    public function toAst() : DropStmt;

    public function toSql() : string;
}
