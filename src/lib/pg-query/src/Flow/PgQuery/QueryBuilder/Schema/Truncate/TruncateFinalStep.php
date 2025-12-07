<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Truncate;

use Flow\PgQuery\Protobuf\AST\TruncateStmt;

interface TruncateFinalStep
{
    public function cascade() : self;

    public function continueIdentity() : self;

    public function restartIdentity() : self;

    public function restrict() : self;

    public function toAst() : TruncateStmt;
}
