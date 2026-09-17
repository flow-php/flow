<?php

declare(strict_types=1);

namespace Flow\ETL\Plan;

use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Plan\Node\Read;

final readonly class LogicalPlan
{
    /**
     * @param Node $root the one root. A DataFrame builds a Result over its chain, or Outputs whose first
     *                   child is that Result and whose other children are sink roots sharing the chain's nodes
     */
    public function __construct(
        public Node $root,
    ) {}

    public static function of(Node $cursor, Sinks $sinks = new Sinks()): self
    {
        $result = new Node\Result($cursor);

        return new self($sinks->count() === 0 ? $result : new Node\Outputs($result, $sinks));
    }

    /**
     * The row chain under the Result: what the next verb builds on.
     *
     * @throws InvalidLogicException when the root is not a Result or Outputs over one
     */
    public function cursor(): Node
    {
        $result = $this->root instanceof Node\Outputs ? $this->root->children()[0] : $this->root;

        if (!$result instanceof Node\Result) {
            throw InvalidLogicException::because('A logical plan must have a Result root, %s found', $result::class);
        }

        return $result->children()[0];
    }

    /**
     * @throws InvalidLogicException when the root is not a Result or Outputs over one
     */
    public function withCursor(Node $cursor): self
    {
        return self::of($cursor, $this->sinks());
    }

    /**
     * @throws InvalidLogicException when the root is not a Result or Outputs over one
     */
    public function withSinks(Sinks $sinks): self
    {
        return self::of($this->cursor(), $this->sinks()->merge($sinks));
    }

    /**
     * The sink roots beside the Result, in the order they were attached.
     */
    public function sinks(): Sinks
    {
        return $this->root instanceof Node\Outputs ? $this->root->sinks() : new Sinks();
    }

    /**
     * The node each consumer reads - a consumer being the Result, a Write, or each Write of a Transaction. A
     * consumer takes rows out of the plan, so a walk from it starts at its input.
     *
     * @return list<Node>
     */
    public function consumerInputs(): array
    {
        $inputs = [];

        foreach ($this->root instanceof Node\Outputs ? $this->root->children() : [$this->root] as $consumer) {
            foreach ($consumer instanceof Node\Transaction ? $consumer->children() : [$consumer] as $write) {
                $inputs[] = $write->children()[0];
            }
        }

        return $inputs;
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
     * can build (a SideInput never sits on the chain), so the walk stops there and never descends into an
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
