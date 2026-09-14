<?php

declare(strict_types=1);

namespace Flow\ETL\Plan;

/**
 * @internal
 */
final readonly class ReplaceLeaf implements Rewrite
{
    public function __construct(
        private Node $target,
        private Node $replacement,
    ) {}

    public function of(Node $node): Node
    {
        return $node === $this->target ? $this->replacement : $node;
    }
}
