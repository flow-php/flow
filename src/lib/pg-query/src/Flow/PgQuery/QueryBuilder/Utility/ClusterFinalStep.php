<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Utility;

use Flow\PgQuery\Protobuf\AST\ClusterStmt;

interface ClusterFinalStep
{
    public function table(string $table) : self;

    public function toAst() : ClusterStmt;

    public function using(string $index) : self;

    public function verbose() : self;
}
