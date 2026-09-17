<?php

declare(strict_types=1);

namespace Flow\ETL\Plan\Explain;

use Flow\ETL\Plan\Node;

/**
 * A node as explain prints it: numbered in execution order, every later visit is a shared reference with no children.
 */
final readonly class Entry
{
    /**
     * @param list<Entry> $children
     */
    public function __construct(
        public Node $node,
        public ?int $number,
        public bool $shared,
        public array $children,
    ) {}

    /**
     * The name with the number in front; a node without a number - Outputs, which only groups the consumers - has none.
     */
    public function title(string $name): string
    {
        return $this->number === null ? $name : '#' . $this->number . ' ' . $name;
    }
}
