<?php

declare(strict_types=1);

namespace Flow\PgQuery\AST\Visitors;

use Flow\PgQuery\AST\NodeVisitor;
use Flow\PgQuery\Protobuf\AST\ColumnRef;

/**
 * A visitor that collects all ColumnRef (column references) nodes.
 */
final class ColumnRefCollector implements NodeVisitor
{
    /**
     * @var array<ColumnRef>
     */
    private array $columnRefs = [];

    public static function nodeClass() : string
    {
        return ColumnRef::class;
    }

    public function enter(object $node) : ?int
    {
        /** @var ColumnRef $node */
        $this->columnRefs[] = $node;

        return null;
    }

    /**
     * @return array<ColumnRef>
     */
    public function getColumnRefs() : array
    {
        return $this->columnRefs;
    }

    public function leave(object $node) : ?int
    {
        return null;
    }

    public function reset() : void
    {
        $this->columnRefs = [];
    }
}
