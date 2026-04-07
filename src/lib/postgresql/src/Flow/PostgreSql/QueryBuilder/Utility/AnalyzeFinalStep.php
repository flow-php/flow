<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Utility;

use Flow\PostgreSql\Protobuf\AST\VacuumStmt;
use Flow\PostgreSql\QueryBuilder\Sql;

interface AnalyzeFinalStep extends Sql
{
    public function skipLocked() : self;

    public function table(string $table, string ...$columns) : self;

    public function tables(string ...$tables) : self;

    public function toAst() : VacuumStmt;

    public function toSql() : string;

    public function verbose() : self;
}
