<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\DropTable;

use Flow\PostgreSql\Protobuf\AST\DropStmt;
use Flow\PostgreSql\QueryBuilder\Sql;

interface DropTableFinalStep extends Sql
{
    public function cascade(): self;

    public function ifExists(): self;

    public function restrict(): self;

    public function toAst(): DropStmt;

    public function toSql(): string;
}
