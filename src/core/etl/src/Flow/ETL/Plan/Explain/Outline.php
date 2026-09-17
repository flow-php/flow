<?php

declare(strict_types=1);

namespace Flow\ETL\Plan\Explain;

use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\Outputs;
use SplObjectStorage;

use function count;

final readonly class Outline
{
    public function of(Node $root): Entry
    {
        /** @var SplObjectStorage<Node, int> $numbers */
        $numbers = new SplObjectStorage();

        return $this->entry($root, $numbers);
    }

    /**
     * A node is numbered once everything it reads is, so the numbers follow the order rows move: the source is #1.
     *
     * @param SplObjectStorage<Node, int> $numbers the nodes numbered so far
     */
    public function entry(Node $node, SplObjectStorage $numbers): Entry
    {
        if ($numbers->offsetExists($node)) {
            return new Entry($node, $numbers[$node], true, []);
        }

        $children = [];

        foreach ($node->children() as $child) {
            $children[] = $this->entry($child, $numbers);
        }

        if ($node instanceof Outputs) {
            return new Entry($node, null, false, $children);
        }

        $numbers[$node] = $number = count($numbers) + 1;

        return new Entry($node, $number, false, $children);
    }
}
