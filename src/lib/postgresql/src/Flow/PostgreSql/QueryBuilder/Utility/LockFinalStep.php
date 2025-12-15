<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Utility;

use Flow\PostgreSql\Protobuf\AST\LockStmt;
use Flow\PostgreSql\QueryBuilder\SqlQuery;

interface LockFinalStep extends SqlQuery
{
    public function accessExclusive() : self;

    public function accessShare() : self;

    public function exclusive() : self;

    public function inMode(LockMode $mode) : self;

    public function nowait() : self;

    public function rowExclusive() : self;

    public function rowShare() : self;

    public function share() : self;

    public function shareRowExclusive() : self;

    public function shareUpdateExclusive() : self;

    public function toAst() : LockStmt;

    public function toSql() : string;
}
