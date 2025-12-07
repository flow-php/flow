<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\CreateTableAs;

use Flow\PgQuery\Protobuf\AST\CreateTableAsStmt;

interface CreateTableAsFinalStep
{
    public function columnNames(string ...$names) : self;

    public function ifNotExists() : self;

    public function toAst() : CreateTableAsStmt;

    public function withNoData() : self;
}
