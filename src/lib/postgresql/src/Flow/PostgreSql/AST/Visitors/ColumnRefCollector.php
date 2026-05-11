<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Visitors;

use Flow\PostgreSql\AST\NodeVisitor;
use Flow\PostgreSql\Protobuf\AST\ColumnRef;

/**
 * A visitor that collects all ColumnRef (column references) nodes.
 */
final class ColumnRefCollector implements NodeVisitor
{
    /**
     * @var array<ColumnRef>
     */
    private array $columnRefs = [];

    public static function nodeClasses(): array
    {
        return [ColumnRef::class];
    }

    public function enter(object $node): ?int
    {
        /** @var ColumnRef $node */
        $this->columnRefs[] = $node;

        return null;
    }

    /**
     * @return array<ColumnRef>
     */
    public function getColumnRefs(): array
    {
        return $this->columnRefs;
    }

    public function leave(object $node): ?int
    {
        return null;
    }

    public function reset(): void
    {
        $this->columnRefs = [];
    }
}
