<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Index\DropIndex;

use Flow\PostgreSql\Protobuf\AST\DropStmt;
use Flow\PostgreSql\QueryBuilder\Sql;

interface DropIndexFinalStep extends Sql
{
    public function cascade(): self;

    public function concurrently(): self;

    public function ifExists(): self;

    public function restrict(): self;

    public function toAst(): DropStmt;

    public function toSql(): string;
}
