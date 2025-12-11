<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Utility;

use Flow\PgQuery\Protobuf\AST\VacuumStmt;

interface VacuumFinalStep
{
    public function analyze() : self;

    public function disablePageSkipping() : self;

    public function freeze() : self;

    public function full() : self;

    public function indexCleanup(IndexCleanup $cleanup) : self;

    public function parallel(int $workers) : self;

    public function processMain(bool $enabled) : self;

    public function processToast(bool $enabled) : self;

    public function skipLocked() : self;

    public function table(string $table, string ...$columns) : self;

    public function tables(string ...$tables) : self;

    public function toAst() : VacuumStmt;

    public function toSql() : string;

    public function truncate(bool $enabled) : self;

    public function verbose() : self;
}
