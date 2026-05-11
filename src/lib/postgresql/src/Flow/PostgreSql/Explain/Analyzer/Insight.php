<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Explain\Analyzer;

use Flow\PostgreSql\Explain\Plan\PlanNode;

final readonly class Insight
{
    /**
     * @param array<string, mixed> $metrics
     */
    public function __construct(
        public InsightType $type,
        public InsightSeverity $severity,
        public string $description,
        public PlanNode $node,
        public array $metrics = [],
    ) {}
}
