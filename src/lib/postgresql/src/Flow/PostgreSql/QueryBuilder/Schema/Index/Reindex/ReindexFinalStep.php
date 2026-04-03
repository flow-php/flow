<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Index\Reindex;

use Flow\PostgreSql\Protobuf\AST\ReindexStmt;
use Flow\PostgreSql\QueryBuilder\Sql;

interface ReindexFinalStep extends Sql
{
    public function concurrently() : self;

    public function tablespace(string $tablespace) : self;

    public function toAst() : ReindexStmt;

    public function toSql() : string;

    public function verbose() : self;
}
