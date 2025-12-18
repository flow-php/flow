<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Explain\Plan;

final readonly class Plan
{
    public function __construct(
        private PlanNode $rootNode,
        private ?float $planningTime = null,
        private ?float $executionTime = null,
        private ?int $memoryUsed = null,
        private ?int $memoryPeak = null,
    ) {
    }

    /**
     * @return array<PlanNode>
     */
    public function allNodes() : array
    {
        return $this->flattenNodes($this->rootNode);
    }

    public function executionTime() : ?float
    {
        return $this->executionTime;
    }

    public function memoryPeak() : ?int
    {
        return $this->memoryPeak;
    }

    public function memoryUsed() : ?int
    {
        return $this->memoryUsed;
    }

    /**
     * @return array<PlanNode>
     */
    public function nodesByType(PlanNodeType $type) : array
    {
        return \array_filter(
            $this->allNodes(),
            static fn (PlanNode $node) : bool => $node->nodeType() === $type
        );
    }

    public function planningTime() : ?float
    {
        return $this->planningTime;
    }

    public function rootNode() : PlanNode
    {
        return $this->rootNode;
    }

    public function totalCost() : float
    {
        return $this->rootNode->cost()->totalCost();
    }

    public function totalTime() : ?float
    {
        if ($this->planningTime === null || $this->executionTime === null) {
            return null;
        }

        return $this->planningTime + $this->executionTime;
    }

    /**
     * @return array<PlanNode>
     */
    private function flattenNodes(PlanNode $node) : array
    {
        $nodes = [$node];

        foreach ($node->children() as $child) {
            $nodes = [...$nodes, ...$this->flattenNodes($child)];
        }

        return $nodes;
    }
}
