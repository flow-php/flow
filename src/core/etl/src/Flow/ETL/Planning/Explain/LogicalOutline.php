<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Explain;

use Flow\ETL\Planning\Logical\Node;
use Flow\ETL\Planning\Logical\Node\JoinsFrame;
use Flow\ETL\Planning\Logical\Node\Outputs;
use Flow\ETL\Planning\Logical\Node\Result;
use Flow\Telemetry\ObjectExtractor;
use SplObjectStorage;

use function count;

final readonly class LogicalOutline
{
    /**
     * @param bool $declarations the declarations optimizer rules read on every title line, in place of the notes
     *                           that say the same in plain words
     */
    public function __construct(
        private NodeDetails $details = new NodeDetails(),
        private bool $declarations = false,
    ) {}

    public function of(Node $root): RenderedNode
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
    public function entry(Node $node, SplObjectStorage $numbers): RenderedNode
    {
        if ($numbers->offsetExists($node)) {
            return $this->describe($node, $numbers[$node], [])->with([], shared: true);
        }

        $children = [];

        foreach ($node->children() as $child) {
            $children[] = $this->entry($this->read($node, $child), $numbers);
        }

        if ($node instanceof Outputs) {
            return $this->describe($node, null, $children);
        }

        $numbers[$node] = $number = count($numbers) + 1;

        return $this->describe($node, $number, $children);
    }

    /**
     * @param list<RenderedNode> $children
     */
    public function describe(Node $node, ?int $number, array $children): RenderedNode
    {
        return new RenderedNode(
            $node,
            ObjectExtractor::shortName($node),
            $this->declarations ? $this->details->labelled($node) : $this->details->lines($node),
            $number,
            false,
            $children,
            $this->declarations ? $this->details->declarations($node) : '',
        );
    }

    /**
     * A join's right side has to be a plan root, and a Result there hands its rows to the join and to nothing else,
     * so the join is drawn reading what that Result reads. An Outputs stays: it means the side has other consumers.
     */
    public function read(Node $node, Node $child): Node
    {
        return $node instanceof JoinsFrame && $child instanceof Result ? $child->children()[0] : $child;
    }
}
