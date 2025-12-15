<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\CreateTableAs;

use Flow\PostgreSql\Protobuf\AST\CreateTableAsStmt;
use Flow\PostgreSql\QueryBuilder\SqlQuery;

interface CreateTableAsFinalStep extends SqlQuery
{
    public function columnNames(string ...$names) : self;

    public function ifNotExists() : self;

    public function toAst() : CreateTableAsStmt;

    public function toSql() : string;

    public function withNoData() : self;
}
