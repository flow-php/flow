<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Grant;

use Flow\PostgreSql\Protobuf\AST\GrantStmt;
use Flow\PostgreSql\QueryBuilder\Sql;

interface RevokeFinalStep extends Sql
{
    public function cascade(): self;

    public function restrict(): self;

    public function toAst(): GrantStmt;

    public function toSql(): string;
}
