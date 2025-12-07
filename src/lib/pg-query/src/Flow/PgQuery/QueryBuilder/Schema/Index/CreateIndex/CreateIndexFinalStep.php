<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Schema\Index\CreateIndex;

use Flow\PgQuery\Protobuf\AST\IndexStmt;
use Flow\PgQuery\QueryBuilder\Condition\Condition;

interface CreateIndexFinalStep
{
    public function include(string ...$columns) : self;

    public function nullsDistinct() : self;

    public function nullsNotDistinct() : self;

    public function tablespace(string $tablespace) : self;

    public function toAst() : IndexStmt;

    public function where(Condition $predicate) : self;
}
