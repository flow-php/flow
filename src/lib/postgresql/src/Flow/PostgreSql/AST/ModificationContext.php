<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST;

/**
 * Provides context information during AST modification.
 *
 * This class gives modifiers access to the traversal state, including
 * parent nodes and depth in the tree, enabling context-aware modifications.
 */
final readonly class ModificationContext
{
    /**
     * @param array<object> $ancestors Stack of parent nodes (from root to immediate parent)
     * @param int $depth Current depth in the AST (1-based, root statements are at depth 1)
     */
    public function __construct(
        private array $ancestors,
        private int $depth,
    ) {
    }

    /**
     * @return array<object>
     */
    public function ancestors() : array
    {
        return $this->ancestors;
    }

    public function depth() : int
    {
        return $this->depth;
    }

    public function isTopLevel() : bool
    {
        return $this->depth === 1;
    }

    public function parent() : ?object
    {
        return $this->ancestors[\count($this->ancestors) - 1] ?? null;
    }
}
