<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Utility;

use Flow\PostgreSql\Protobuf\AST\ClusterStmt;
use Flow\PostgreSql\QueryBuilder\Sql;

interface ClusterFinalStep extends Sql
{
    public function table(string $table) : self;

    public function toAst() : ClusterStmt;

    public function toSql() : string;

    public function using(string $index) : self;

    public function verbose() : self;
}
