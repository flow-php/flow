<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Schema\Index\CreateIndex;

use Flow\PostgreSql\Protobuf\AST\IndexStmt;
use Flow\PostgreSql\QueryBuilder\Condition\Condition;

interface CreateIndexFinalStep
{
    public function include(string ...$columns) : self;

    public function nullsDistinct() : self;

    public function nullsNotDistinct() : self;

    public function tablespace(string $tablespace) : self;

    public function toAst() : IndexStmt;

    public function toSql() : string;

    public function where(Condition $predicate) : self;
}
