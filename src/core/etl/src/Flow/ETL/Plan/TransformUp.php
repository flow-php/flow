<?php

declare(strict_types=1);

namespace Flow\ETL\Plan;

use SplObjectStorage;

/**
 * @internal the bottom-up rebuild; extracted so LogicalPlan needs no private recursion
 */
final readonly class TransformUp
{
    /**
     * One per LogicalPlan::transformUp() call, so a node reached from several roots is rewritten once and
     * keeps its identity.
     *
     * @var SplObjectStorage<Node, Node>
     */
    private SplObjectStorage $memo;

    public function __construct()
    {
        /** @var SplObjectStorage<Node, Node> $memo */
        $memo = new SplObjectStorage();
        $this->memo = $memo;
    }

    public function of(Node $node, Rewrite $rewrite): Node
    {
        if ($this->memo->contains($node)) {
            return $this->memo[$node];
        }

        $children = [];

        foreach ($node->children() as $child) {
            $children[] = $this->of($child, $rewrite);
        }

        return $this->memo[$node] = $rewrite->of($node->withChildren($children));
    }
}
