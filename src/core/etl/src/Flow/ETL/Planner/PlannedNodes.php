<?php

declare(strict_types=1);

namespace Flow\ETL\Planner;

use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Loader;
use Flow\ETL\Plan\Node;
use Flow\ETL\Processor;
use Flow\ETL\Transformer;
use SplObjectStorage;

/**
 * What one Planner::plan() call has planned so far: every node by identity, and the first schema refusal.
 */
final class PlannedNodes
{
    /**
     * @var SplObjectStorage<Node, PlannedNode>
     */
    private SplObjectStorage $nodes;

    private ?SchemaNotDerivableException $refusal = null;

    public function __construct()
    {
        /** @var SplObjectStorage<Node, PlannedNode> $nodes */
        $nodes = new SplObjectStorage();
        $this->nodes = $nodes;
    }

    public function add(Node $node, PlannedNode $planned): PlannedNode
    {
        $this->nodes[$node] = $planned;

        return $planned;
    }

    public function has(Node $node): bool
    {
        return $this->nodes->offsetExists($node);
    }

    /**
     * @throws InvalidLogicException when $node was never planned
     */
    public function of(Node $node): PlannedNode
    {
        return $this->nodes->offsetExists($node)
            ? $this->nodes[$node]
            : throw InvalidLogicException::because('Node %s was never planned', $node::class);
    }

    /**
     * Keeps the first refusal. A nested plan planned after a refusing sibling carries the OUTER refusal, by design.
     */
    public function refuse(SchemaNotDerivableException $refusal): void
    {
        $this->refusal ??= $refusal;
    }

    /**
     * Non-null means the WHOLE plan runs raw - all or nothing.
     */
    public function refusal(): ?SchemaNotDerivableException
    {
        return $this->refusal;
    }

    /**
     * The steps a pipeline runs for $node: the bound ones, or the raw ones when any node of the plan refused a
     * schema - a refusal is plan-wide, so every node answers the same way.
     *
     * @throws InvalidLogicException when $node was never planned
     *
     * @return list<Loader|Processor|Transformer>
     */
    public function steps(Node $node): array
    {
        $planned = $this->of($node);

        return $this->refusal === null ? $planned->bound : $planned->steps;
    }
}
