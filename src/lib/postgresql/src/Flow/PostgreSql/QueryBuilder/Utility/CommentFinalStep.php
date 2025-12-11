<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Utility;

use Flow\PostgreSql\Protobuf\AST\CommentStmt;

interface CommentFinalStep
{
    public function is(string $comment) : self;

    public function isNull() : self;

    public function toAst() : CommentStmt;

    public function toSql() : string;
}
