<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Database;

use Flow\PostgreSql\Protobuf\AST\DropdbStmt;
use Flow\PostgreSql\QueryBuilder\Sql;

interface DropDatabaseFinalStep extends Sql
{
    public function force(): self;

    public function ifExists(): self;

    public function toAst(): DropdbStmt;

    public function toSql(): string;
}
