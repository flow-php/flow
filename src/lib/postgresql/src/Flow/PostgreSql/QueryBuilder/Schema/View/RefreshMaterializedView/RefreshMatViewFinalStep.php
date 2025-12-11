<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\View\RefreshMaterializedView;

use Flow\PostgreSql\Protobuf\AST\RefreshMatViewStmt;

interface RefreshMatViewFinalStep
{
    public function toAst() : RefreshMatViewStmt;

    public function toSql() : string;

    public function withData() : self;

    public function withNoData() : self;
}
