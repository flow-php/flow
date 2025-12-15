<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Truncate;

use Flow\PostgreSql\Protobuf\AST\TruncateStmt;
use Flow\PostgreSql\QueryBuilder\SqlQuery;

interface TruncateFinalStep extends SqlQuery
{
    public function cascade() : self;

    public function continueIdentity() : self;

    public function restartIdentity() : self;

    public function restrict() : self;

    public function toAst() : TruncateStmt;

    public function toSql() : string;
}
