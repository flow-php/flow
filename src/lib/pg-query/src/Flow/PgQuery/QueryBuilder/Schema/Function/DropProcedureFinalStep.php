<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Function;

use Flow\PgQuery\Protobuf\AST\DropStmt;

interface DropProcedureFinalStep
{
    public function arguments(FunctionArgument ...$args) : self;

    public function cascade() : self;

    public function ifExists() : self;

    public function restrict() : self;

    public function toAst() : DropStmt;

    public function toSql() : string;
}
