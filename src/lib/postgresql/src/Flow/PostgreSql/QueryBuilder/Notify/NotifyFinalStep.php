<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Notify;

use Flow\PostgreSql\Protobuf\AST\NotifyStmt;
use Flow\PostgreSql\QueryBuilder\Sql;

interface NotifyFinalStep extends Sql
{
    public function toAst() : NotifyStmt;

    public function withPayload(string $payload) : self;
}
