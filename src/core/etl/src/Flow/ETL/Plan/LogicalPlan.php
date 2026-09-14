<?php

declare(strict_types=1);

namespace Flow\ETL\Plan;

use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Plan\Node\Read;

use function array_slice;

/**
 * @type Sinks = list<Node\Write|Node\Transaction>
 */
final readonly class LogicalPlan
{
    /**
     * @param Node $root the one root. A DataFrame builds a Result over its chain, or a SinkMultiple whose first
     *                   child is that Result and whose other children are sink roots sharing the chain's nodes
     */
    public function __construct(
        public Node $root,
    ) {}

    public function withRoot(Node $root): self
    {
        return new self($root);
    }

    /**
     * The sink roots beside the Result, in the order they were attached.
     *
     * @return Sinks
     */
    public function sinkRoots(): array
    {
        /** @var Sinks */
        return $this->root instanceof Node\SinkMultiple ? array_slice($this->root->children(), 1) : [];
    }

    /**
     * The node under every consumer - a Result, a Write, or each Write of a Transaction. A consumer is a root,
     * not a pass-through, so a walk from it starts below it.
     *
     * @return list<Node>
     */
    public function consumers(): array
    {
        $consumers = [];

        foreach ($this->root instanceof Node\SinkMultiple ? $this->root->children() : [$this->root] as $consumer) {
            foreach ($consumer instanceof Node\Transaction ? $consumer->children() : [$consumer] as $write) {
                $consumers[] = $write->children()[0];
            }
        }

        return $consumers;
    }

    /**
     * ONE memo for the whole DAG, so a prefix shared by the Result and a sink is rewritten once.
     */
    public function transformUp(Rewrite $rewrite): self
    {
        return new self((new TransformUp())->of($this->root, $rewrite));
    }

    /**
     * This frame's own source: follow children()[0] to the leaf. Read is the only leaf kind a DataFrame
     * can build (a Frame is always a side input), so the walk stops there and never descends into an
     * embedded plan.
     *
     * @throws InvalidLogicException when the row-input chain does not end in a Read
     */
    public function source(): Read
    {
        $node = $this->root;

        while (!$node instanceof Read) {
            $children = $node->children();

            if ($children === []) {
                throw InvalidLogicException::because('A logical plan must end in a Read, %s found', $node::class);
            }

            $node = $children[0];
        }

        return $node;
    }
}
