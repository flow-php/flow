<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Utility;

use Flow\PgQuery\Protobuf\AST\VacuumStmt;

interface AnalyzeFinalStep
{
    public function skipLocked() : self;

    public function table(string $table, string ...$columns) : self;

    public function tables(string ...$tables) : self;

    public function toAst() : VacuumStmt;

    public function verbose() : self;
}
