<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Utility;

use Flow\PostgreSql\Protobuf\AST\CommentStmt;
use Flow\PostgreSql\QueryBuilder\Sql;

interface CommentFinalStep extends Sql
{
    public function is(string $comment) : self;

    public function isNull() : self;

    public function toAst() : CommentStmt;

    public function toSql() : string;
}
