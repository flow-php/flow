<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Explain\Analyzer;

use Flow\PostgreSql\Explain\Plan\Plan;
use Flow\PostgreSql\Explain\Plan\PlanNode;

use function Flow\Types\DSL\type_float;

final readonly class PlanAnalyzer
{
    public function __construct(
        private Plan $plan,
    ) {}

    /**
     * Get all insights at once.
     *
     * @return array<Insight>
     */
    public function allInsights(): array
    {
        return [
            ...$this->slowestNodes(),
            ...$this->estimateMismatches(),
            ...$this->sequentialScans(),
            ...$this->externalSorts(),
            ...$this->inefficientFilters(),
            ...$this->diskReads(),
            ...$this->lowCacheHits(),
        ];
    }

    /**
     * Get nodes that read from disk (not from shared buffers).
     *
     * @return array<Insight>
     */
    public function diskReads(): array
    {
        $insights = [];

        foreach ($this->plan->allNodes() as $node) {
            $buffers = $node->buffers();

            if ($buffers === null) {
                continue;
            }

            if ($buffers->sharedRead() === 0) {
                continue;
            }

            $blocksRead = $buffers->sharedRead();
            $totalBlocks = $buffers->totalSharedBlocks();
            $diskRatio = $totalBlocks > 0 ? $blocksRead / $totalBlocks : 0.0;

            $severity = match (true) {
                $diskRatio >= 0.5 => InsightSeverity::CRITICAL,
                $diskRatio >= 0.2 => InsightSeverity::WARNING,
                default => InsightSeverity::INFO,
            };

            $insights[] = new Insight(
                type: InsightType::DISK_READ,
                severity: $severity,
                description: \sprintf(
                    '%s on %s read %d blocks from disk (%.1f%% disk reads)',
                    $node->nodeType()->value,
                    $this->nodeIdentifier($node),
                    $blocksRead,
                    $diskRatio * 100,
                ),
                node: $node,
                metrics: [
                    'blocks_read' => $blocksRead,
                    'blocks_hit' => $buffers->sharedHit(),
                    'disk_ratio' => $diskRatio,
                ],
            );
        }

        return $insights;
    }

    /**
     * Get nodes where actual rows differ significantly from estimated.
     *
     * @return array<Insight>
     */
    public function estimateMismatches(float $threshold = 2.0): array
    {
        $insights = [];

        foreach ($this->plan->allNodes() as $node) {
            $accuracy = $node->rowEstimateAccuracy();

            if ($accuracy === null) {
                continue;
            }

            $mismatchRatio = $accuracy > 1.0 ? $accuracy : 1.0 / $accuracy;

            if ($mismatchRatio < $threshold) {
                continue;
            }

            $severity = match (true) {
                $mismatchRatio >= 100.0 => InsightSeverity::CRITICAL,
                $mismatchRatio >= 10.0 => InsightSeverity::WARNING,
                default => InsightSeverity::INFO,
            };

            $direction = $accuracy > 1.0 ? 'underestimated' : 'overestimated';
            $insights[] = new Insight(
                type: InsightType::ESTIMATE_MISMATCH,
                severity: $severity,
                description: \sprintf(
                    '%s on %s: %s rows by %.1fx (estimated %d, actual %d)',
                    $node->nodeType()->value,
                    $this->nodeIdentifier($node),
                    $direction,
                    $mismatchRatio,
                    $node->estimatedRows(),
                    $node->actualRows(),
                ),
                node: $node,
                metrics: [
                    'estimated_rows' => $node->estimatedRows(),
                    'actual_rows' => $node->actualRows(),
                    'mismatch_ratio' => $mismatchRatio,
                    'accuracy' => $accuracy,
                ],
            );
        }

        \usort(
            $insights,
            static fn(Insight $a, Insight $b): int => (
                type_float()->assert($b->metrics['mismatch_ratio']) <=> type_float()->assert(
                    $a->metrics['mismatch_ratio'],
                )
            ),
        );

        return $insights;
    }

    /**
     * Get sorts that spilled to disk.
     *
     * @return array<Insight>
     */
    public function externalSorts(): array
    {
        $insights = [];

        foreach ($this->plan->allNodes() as $node) {
            if (!$node->usesExternalSort()) {
                continue;
            }

            $spaceUsed = $node->sortSpaceUsed();

            $insights[] = new Insight(
                type: InsightType::EXTERNAL_SORT,
                severity: InsightSeverity::WARNING,
                description: \sprintf(
                    'Sort operation spilled to disk using %s KB',
                    $spaceUsed !== null ? \number_format($spaceUsed) : 'unknown',
                ),
                node: $node,
                metrics: [
                    'sort_method' => $node->sortMethod(),
                    'space_used_kb' => $spaceUsed,
                    'sort_key' => $node->sortKey(),
                ],
            );
        }

        return $insights;
    }

    /**
     * Get nodes where filter removed a high percentage of rows.
     *
     * @return array<Insight>
     */
    public function inefficientFilters(float $threshold = 0.5): array
    {
        $insights = [];

        foreach ($this->plan->allNodes() as $node) {
            $rowsRemoved = $node->rowsRemovedByFilter();
            $actualRows = $node->actualRows();

            if ($rowsRemoved === null || $actualRows === null) {
                continue;
            }

            $totalRows = $actualRows + $rowsRemoved;

            if ($totalRows === 0) {
                continue;
            }

            $removalRatio = $rowsRemoved / $totalRows;

            if ($removalRatio < $threshold) {
                continue;
            }

            $severity = match (true) {
                $removalRatio >= 0.9 => InsightSeverity::CRITICAL,
                $removalRatio >= 0.7 => InsightSeverity::WARNING,
                default => InsightSeverity::INFO,
            };

            $insights[] = new Insight(
                type: InsightType::INEFFICIENT_FILTER,
                severity: $severity,
                description: \sprintf(
                    '%s on %s: filter removed %d of %d rows (%.1f%%)',
                    $node->nodeType()->value,
                    $this->nodeIdentifier($node),
                    $rowsRemoved,
                    $totalRows,
                    $removalRatio * 100,
                ),
                node: $node,
                metrics: [
                    'rows_removed' => $rowsRemoved,
                    'rows_kept' => $actualRows,
                    'total_rows' => $totalRows,
                    'removal_ratio' => $removalRatio,
                    'filter' => $node->filter(),
                ],
            );
        }

        \usort(
            $insights,
            static fn(Insight $a, Insight $b): int => (
                type_float()->assert($b->metrics['removal_ratio']) <=> type_float()->assert(
                    $a->metrics['removal_ratio'],
                )
            ),
        );

        return $insights;
    }

    /**
     * Get nodes with low cache hit ratio.
     *
     * @return array<Insight>
     */
    public function lowCacheHits(float $threshold = 0.9): array
    {
        $insights = [];

        foreach ($this->plan->allNodes() as $node) {
            $buffers = $node->buffers();

            if ($buffers === null) {
                continue;
            }

            if ($buffers->totalSharedBlocks() === 0) {
                continue;
            }

            $hitRatio = $buffers->hitRatio();

            if ($hitRatio >= $threshold) {
                continue;
            }

            $severity = match (true) {
                $hitRatio < 0.5 => InsightSeverity::CRITICAL,
                $hitRatio < 0.8 => InsightSeverity::WARNING,
                default => InsightSeverity::INFO,
            };

            $insights[] = new Insight(
                type: InsightType::LOW_CACHE_HIT,
                severity: $severity,
                description: \sprintf(
                    '%s on %s: cache hit ratio %.1f%% (hit %d, read %d)',
                    $node->nodeType()->value,
                    $this->nodeIdentifier($node),
                    $hitRatio * 100,
                    $buffers->sharedHit(),
                    $buffers->sharedRead(),
                ),
                node: $node,
                metrics: [
                    'hit_ratio' => $hitRatio,
                    'shared_hit' => $buffers->sharedHit(),
                    'shared_read' => $buffers->sharedRead(),
                ],
            );
        }

        \usort(
            $insights,
            static fn(Insight $a, Insight $b): int => (
                type_float()->assert($a->metrics['hit_ratio']) <=> type_float()->assert($b->metrics['hit_ratio'])
            ),
        );

        return $insights;
    }

    /**
     * Get all sequential scan nodes.
     *
     * @return array<Insight>
     */
    public function sequentialScans(): array
    {
        $insights = [];

        foreach ($this->plan->allNodes() as $node) {
            if (!$node->isSequentialScan()) {
                continue;
            }

            $actualRows = $node->actualRows();
            $estimatedRows = $node->estimatedRows();

            $severity = match (true) {
                $estimatedRows >= 10000 => InsightSeverity::WARNING,
                $estimatedRows >= 1000 => InsightSeverity::INFO,
                default => InsightSeverity::INFO,
            };

            $insights[] = new Insight(
                type: InsightType::SEQUENTIAL_SCAN,
                severity: $severity,
                description: \sprintf(
                    'Sequential scan on %s (%s rows)',
                    $this->nodeIdentifier($node),
                    $actualRows !== null ? \number_format($actualRows) : \number_format($estimatedRows) . ' estimated',
                ),
                node: $node,
                metrics: [
                    'estimated_rows' => $estimatedRows,
                    'actual_rows' => $actualRows,
                    'relation' => $node->relationName(),
                    'filter' => $node->filter(),
                ],
            );
        }

        return $insights;
    }

    /**
     * Get nodes sorted by execution time (slowest first).
     *
     * @return array<Insight>
     */
    public function slowestNodes(int $limit = 5): array
    {
        $nodesWithTiming = [];

        foreach ($this->plan->allNodes() as $node) {
            $timing = $node->timing();

            if ($timing === null) {
                continue;
            }

            $nodesWithTiming[] = $node;
        }

        \usort(
            $nodesWithTiming,
            static fn(PlanNode $a, PlanNode $b): int => (
                (int) ($b->timing()?->totalActualTime() ?? 0.0) <=> (int) ($a->timing()?->totalActualTime() ?? 0.0)
            ),
        );

        $topNodes = \array_slice($nodesWithTiming, 0, $limit);
        $insights = [];

        $executionTime = $this->plan->executionTime();

        foreach ($topNodes as $node) {
            $timing = $node->timing();

            if ($timing === null) {
                continue;
            }

            $nodeTime = $timing->totalActualTime();
            $percentage = $executionTime !== null && $executionTime > 0 ? ($nodeTime / $executionTime) * 100 : null;

            $severity = match (true) {
                $percentage !== null && $percentage >= 50.0 => InsightSeverity::CRITICAL,
                $percentage !== null && $percentage >= 25.0 => InsightSeverity::WARNING,
                default => InsightSeverity::INFO,
            };

            $insights[] = new Insight(
                type: InsightType::SLOW_NODE,
                severity: $severity,
                description: \sprintf(
                    '%s on %s: %.3f ms%s',
                    $node->nodeType()->value,
                    $this->nodeIdentifier($node),
                    $nodeTime,
                    $percentage !== null ? \sprintf(' (%.1f%% of total)', $percentage) : '',
                ),
                node: $node,
                metrics: [
                    'time_ms' => $nodeTime,
                    'percentage' => $percentage,
                    'loops' => $node->actualLoops(),
                ],
            );
        }

        return $insights;
    }

    /**
     * Get summary statistics for the plan.
     */
    public function summary(): PlanSummary
    {
        $nodes = $this->plan->allNodes();
        $sequentialScanCount = 0;
        $indexScanCount = 0;
        $hasExternalSort = false;
        $hasDiskReads = false;
        $totalSharedHit = 0;
        $totalSharedRead = 0;
        $hashJoinCount = 0;
        $nestedLoopCount = 0;
        $mergeJoinCount = 0;
        $hasTempSpill = false;

        foreach ($nodes as $node) {
            if ($node->isSequentialScan()) {
                $sequentialScanCount++;
            }

            if ($node->isIndexScan()) {
                $indexScanCount++;
            }

            if ($node->usesExternalSort()) {
                $hasExternalSort = true;
            }

            if ($node->isHashJoin()) {
                $hashJoinCount++;
            }

            if ($node->isNestedLoop()) {
                $nestedLoopCount++;
            }

            if ($node->isMergeJoin()) {
                $mergeJoinCount++;
            }

            $buffers = $node->buffers();

            if ($buffers !== null) {
                if ($buffers->sharedRead() > 0) {
                    $hasDiskReads = true;
                }

                if ($buffers->hasDiskSpill()) {
                    $hasTempSpill = true;
                }

                $totalSharedHit += $buffers->sharedHit();
                $totalSharedRead += $buffers->sharedRead();
            }
        }

        $totalBlocks = $totalSharedHit + $totalSharedRead;
        $overallCacheHitRatio = $totalBlocks > 0 ? $totalSharedHit / $totalBlocks : null;

        return new PlanSummary(
            totalCost: $this->plan->totalCost(),
            executionTime: $this->plan->executionTime(),
            planningTime: $this->plan->planningTime(),
            nodeCount: \count($nodes),
            sequentialScanCount: $sequentialScanCount,
            indexScanCount: $indexScanCount,
            hasExternalSort: $hasExternalSort,
            hasDiskReads: $hasDiskReads,
            overallCacheHitRatio: $overallCacheHitRatio,
            memoryUsed: $this->plan->memoryUsed(),
            memoryPeak: $this->plan->memoryPeak(),
            hashJoinCount: $hashJoinCount,
            nestedLoopCount: $nestedLoopCount,
            mergeJoinCount: $mergeJoinCount,
            totalSharedHit: $totalSharedHit,
            totalSharedRead: $totalSharedRead,
            hasTempSpill: $hasTempSpill,
            estimatedRows: $this->plan->rootNode()->estimatedRows(),
            actualRows: $this->plan->rootNode()->actualRows(),
        );
    }

    private function nodeIdentifier(PlanNode $node): string
    {
        $relationName = $node->relationName();

        if ($relationName !== null) {
            $identifier = $relationName;
            $alias = $node->alias();

            if ($alias !== null && $alias !== $relationName) {
                $identifier .= ' (' . $alias . ')';
            }

            return "'" . $identifier . "'";
        }

        $indexName = $node->indexName();

        if ($indexName !== null) {
            return "'" . $indexName . "'";
        }

        return 'node';
    }
}
