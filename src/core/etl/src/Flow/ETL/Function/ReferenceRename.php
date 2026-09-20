<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row\Reference;
use Flow\ETL\Row\UnresolvedReference;

/**
 * Points every reference to $from in a function tree at $to instead; the rest of the tree is untouched.
 */
final readonly class ReferenceRename
{
    public function __construct(
        private string $from,
        private string $to,
    ) {}

    /**
     * null when a reference to $from is a resolved one - only an unresolved reference can be pointed elsewhere
     * without a type it no longer matches.
     */
    public function in(FunctionTree $tree): ?FunctionTree
    {
        if ($tree instanceof Reference && $tree->to() === $this->from) {
            if (!$tree instanceof UnresolvedReference) {
                return null;
            }

            $renamed = new UnresolvedReference($this->to);

            return $tree->hasAlias() ? $renamed->as($tree->name()) : $renamed;
        }

        $children = [];

        foreach ($tree->children() as $child) {
            $renamed = $this->in($child);

            if ($renamed === null) {
                return null;
            }

            $children[] = $renamed;
        }

        return $children === $tree->children() ? $tree : $tree->withChildren($children);
    }
}
