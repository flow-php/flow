<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Logical;

use Flow\ETL\Foundation\Exception\InvalidLogicException;
use Flow\ETL\Planning\Logical\Node\Read;
use SplObjectStorage;

use function array_pop;
use function array_push;

final readonly class LogicalPlan
{
    /**
     * The one root, built by the Trigger that runs the plan.
     */
    public function __construct(
        public Node $root,
    ) {}

    /**
     * True when every source the plan reads, a joined frame's included, can be read again.
     */
    public function isRepeatable(): bool
    {
        foreach ($this->reads() as $read) {
            if (!$read->extractor()->isRepeatable()) {
                return false;
            }
        }

        return true;
    }

    /**
     * Every Read of the plan once, a joined frame's included.
     *
     * @return list<Read>
     */
    public function reads(): array
    {
        /** @var SplObjectStorage<Node, null> $seen */
        $seen = new SplObjectStorage();
        $stack = [$this->root];
        $reads = [];

        while ($stack !== []) {
            $node = array_pop($stack);

            if ($seen->offsetExists($node)) {
                continue;
            }

            $seen[$node] = null;

            if ($node instanceof Read) {
                $reads[] = $node;
            }

            array_push($stack, ...$node->children());
        }

        return $reads;
    }

    /**
     * The write roots this plan's root carries, in the order they were attached.
     */
    public function writes(): Writes
    {
        return match (true) {
            $this->root instanceof Node\Outputs => $this->root->writes(),
            $this->root instanceof Node\Write, $this->root instanceof Node\Transaction => new Writes($this->root),
            default => new Writes(),
        };
    }

    /**
     * The root's writes, then the writes of every Outputs further down the chain - a frame this plan read keeps its
     * own writes, and they read the rows that pass through it.
     */
    public function writesOnSpine(): Writes
    {
        $writes = $this->writes();

        for ($node = $this->root; $node->children() !== []; $node = $node->children()[0]) {
            if ($node !== $this->root && $node instanceof Node\Outputs) {
                $writes = $writes->merge($node->writes());
            }
        }

        return $writes;
    }

    /**
     * The node the first consumer reads: the chain a verb built, under the consumer the trigger put on top.
     *
     * @throws InvalidLogicException when that consumer is a Transaction, whose children are sibling writes, or when
     *                               the root carries no consumer at all
     */
    public function spine(): Node
    {
        $consumer = $this->root instanceof Node\Outputs ? $this->root->children()[0] : $this->root;

        if ($consumer instanceof Node\Transaction) {
            throw InvalidLogicException::firstConsumerIsATransaction();
        }

        return $consumer->children()[0] ?? throw InvalidLogicException::because(
            'A logical plan must have a consumer root, %s found',
            $consumer::class,
        );
    }

    /**
     * The node each consumer reads - the root's consumers, each Write of a Transaction expanded, and the writes of
     * every Outputs further down the spine. A consumer takes rows out of the plan, so a walk from it starts at its
     * input.
     *
     * @return list<Node>
     */
    public function consumerInputs(): array
    {
        $first = $this->root instanceof Node\Outputs ? $this->root->children()[0] : $this->root;
        $consumers = $first instanceof Node\Result
            ? [$first, ...$this->writesOnSpine()->all()]
            : $this->writesOnSpine()->all();

        $inputs = [];

        foreach ($consumers as $consumer) {
            foreach ($consumer instanceof Node\Transaction ? $consumer->children() : [$consumer] as $write) {
                $inputs[] = $write->children()[0];
            }
        }

        return $inputs;
    }

    /**
     * ONE memo for the whole DAG, so a prefix shared by several consumers is rewritten once. A join's right side
     * is handed back as it is.
     */
    public function transformUp(Rewrite $rewrite): self
    {
        $root = (new TransformUp())->of($this->root, $rewrite);

        return $root === $this->root ? $this : new self($root);
    }

    /**
     * This frame's own source: follow children()[0] to the leaf. Read is the only leaf kind a DataFrame
     * can build, so the walk stops there and never descends into a join's right side.
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
