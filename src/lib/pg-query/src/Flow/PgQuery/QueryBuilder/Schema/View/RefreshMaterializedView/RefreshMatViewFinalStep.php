<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\View\RefreshMaterializedView;

use Flow\PgQuery\Protobuf\AST\RefreshMatViewStmt;

interface RefreshMatViewFinalStep
{
    public function toAst() : RefreshMatViewStmt;

    public function withData() : self;

    public function withNoData() : self;
}
