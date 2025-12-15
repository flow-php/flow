<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\DropSequence;

use Flow\PostgreSql\Protobuf\AST\DropStmt;
use Flow\PostgreSql\QueryBuilder\SqlQuery;

interface DropSequenceFinalStep extends SqlQuery
{
    public function cascade() : self;

    public function ifExists() : self;

    public function restrict() : self;

    public function toAst() : DropStmt;

    public function toSql() : string;
}
