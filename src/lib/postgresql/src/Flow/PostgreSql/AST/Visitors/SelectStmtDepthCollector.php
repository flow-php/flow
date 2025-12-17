<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Visitors;

use Flow\PostgreSql\AST\NodeVisitor;
use Flow\PostgreSql\Protobuf\AST\SelectStmt;

/**
 * A visitor that tracks the maximum nesting depth of SELECT statements.
 */
final class SelectStmtDepthCollector implements NodeVisitor
{
    private int $currentDepth = 0;

    private int $maxDepth = 0;

    public static function nodeClass() : string
    {
        return SelectStmt::class;
    }

    public function enter(object $node) : ?int
    {
        $this->currentDepth++;

        if ($this->currentDepth > $this->maxDepth) {
            $this->maxDepth = $this->currentDepth;
        }

        return null;
    }

    public function getMaxDepth() : int
    {
        return $this->maxDepth;
    }

    public function leave(object $node) : ?int
    {
        $this->currentDepth--;

        return null;
    }

    public function reset() : void
    {
        $this->currentDepth = 0;
        $this->maxDepth = 0;
    }
}
