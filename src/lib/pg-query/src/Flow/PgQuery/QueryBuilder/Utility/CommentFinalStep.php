<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Utility;

use Flow\PgQuery\Protobuf\AST\CommentStmt;

interface CommentFinalStep
{
    public function is(string $comment) : self;

    public function isNull() : self;

    public function toAst() : CommentStmt;
}
