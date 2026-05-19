<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Visitors;

use Flow\PostgreSql\AST\NodeVisitor;
use Flow\PostgreSql\Protobuf\AST\SortBy;

use function count;

/**
 * A visitor that collects all SortBy (ORDER BY items) nodes.
 */
final class SortByCollector implements NodeVisitor
{
    /**
     * @var array<SortBy>
     */
    private array $sortByClauses = [];

    public static function nodeClasses(): array
    {
        return [SortBy::class];
    }

    public function enter(object $node): ?int
    {
        /** @var SortBy $node */
        $this->sortByClauses[] = $node;

        return null;
    }

    /**
     * @return array<SortBy>
     */
    public function getSortByClauses(): array
    {
        return $this->sortByClauses;
    }

    public function hasSortBy(): bool
    {
        return count($this->sortByClauses) > 0;
    }

    public function leave(object $node): ?int
    {
        return null;
    }

    public function reset(): void
    {
        $this->sortByClauses = [];
    }
}
