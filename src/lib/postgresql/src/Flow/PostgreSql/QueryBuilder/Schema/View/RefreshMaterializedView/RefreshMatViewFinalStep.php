<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\View\RefreshMaterializedView;

use Flow\PostgreSql\Protobuf\AST\RefreshMatViewStmt;
use Flow\PostgreSql\QueryBuilder\Sql;

interface RefreshMatViewFinalStep extends Sql
{
    public function toAst() : RefreshMatViewStmt;

    public function toSql() : string;

    public function withData() : self;

    public function withNoData() : self;
}
