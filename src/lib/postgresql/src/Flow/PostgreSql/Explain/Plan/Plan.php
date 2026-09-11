<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Explain\Plan;

use function array_filter;

/**
 * @import-type PlanNodeShape from PlanNode
 */
final readonly class Plan
{
    public function __construct(
        private PlanNode $rootNode,
        private ?float $planningTime = null,
        private ?float $executionTime = null,
        private ?int $memoryUsed = null,
        private ?int $memoryPeak = null,
    ) {}

    /**
     * @param array{
     *     root_node: PlanNodeShape,
     *     planning_time: ?float,
     *     execution_time: ?float,
     *     memory_used: ?int,
     *     memory_peak: ?int
     * } $data
     */
    public static function fromArray(array $data): self
    {
        $rootNodeData = $data['root_node'];

        return new self(
            rootNode: PlanNode::fromArray($rootNodeData),
            planningTime: $data['planning_time'],
            executionTime: $data['execution_time'],
            memoryUsed: $data['memory_used'],
            memoryPeak: $data['memory_peak'],
        );
    }

    /**
     * @return array<PlanNode>
     */
    public function allNodes(): array
    {
        return $this->flattenNodes($this->rootNode);
    }

    public function executionTime(): ?float
    {
        return $this->executionTime;
    }

    public function memoryPeak(): ?int
    {
        return $this->memoryPeak;
    }

    public function memoryUsed(): ?int
    {
        return $this->memoryUsed;
    }

    /**
     * @return array<PlanNode>
     */
    public function nodesByType(PlanNodeType $type): array
    {
        return array_filter($this->allNodes(), static fn(PlanNode $node): bool => $node->nodeType() === $type);
    }

    /**
     * @return array{
     *     root_node: PlanNodeShape,
     *     planning_time: ?float,
     *     execution_time: ?float,
     *     memory_used: ?int,
     *     memory_peak: ?int
     * }
     */
    public function normalize(): array
    {
        return [
            'root_node' => $this->rootNode->normalize(),
            'planning_time' => $this->planningTime,
            'execution_time' => $this->executionTime,
            'memory_used' => $this->memoryUsed,
            'memory_peak' => $this->memoryPeak,
        ];
    }

    public function planningTime(): ?float
    {
        return $this->planningTime;
    }

    public function rootNode(): PlanNode
    {
        return $this->rootNode;
    }

    public function totalCost(): float
    {
        return $this->rootNode->cost()->totalCost();
    }

    public function totalTime(): ?float
    {
        if ($this->planningTime === null || $this->executionTime === null) {
            return null;
        }

        return $this->planningTime + $this->executionTime;
    }

    /**
     * @return array<PlanNode>
     */
    private function flattenNodes(PlanNode $node): array
    {
        $nodes = [$node];

        foreach ($node->children() as $child) {
            $nodes = [...$nodes, ...$this->flattenNodes($child)];
        }

        return $nodes;
    }
}
