<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Index\Reindex;

use Flow\PgQuery\Protobuf\AST\ReindexStmt;

interface ReindexFinalStep
{
    public function concurrently() : self;

    public function tablespace(string $tablespace) : self;

    public function toAst() : ReindexStmt;

    public function toSql() : string;

    public function verbose() : self;
}
