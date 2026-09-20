<?php

declare(strict_types=1);

namespace Flow\ETL\Plan;

use SplObjectStorage;

use function array_keys;

/**
 * The bottom-up rebuild; extracted so LogicalPlan needs no private recursion.
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
        if ($this->memo->offsetExists($node)) {
            return $this->memo[$node];
        }

        $children = $node->children();

        // a joined frame may share nodes with this plan, and a rewrite of this plan must not change what that frame reads
        foreach ($node instanceof Node\JoinsFrame ? [0] : array_keys($children) as $i) {
            $children[$i] = $this->of($children[$i], $rewrite);
        }

        return $this->memo[$node] = $rewrite->of($node->withChildren($children));
    }
}
