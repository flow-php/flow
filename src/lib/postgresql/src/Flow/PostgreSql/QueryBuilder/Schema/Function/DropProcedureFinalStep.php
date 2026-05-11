<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Function;

use Flow\PostgreSql\Protobuf\AST\DropStmt;
use Flow\PostgreSql\QueryBuilder\Sql;

interface DropProcedureFinalStep extends Sql
{
    public function arguments(FunctionArgument ...$args): self;

    public function cascade(): self;

    public function ifExists(): self;

    public function restrict(): self;

    public function toAst(): DropStmt;

    public function toSql(): string;
}
