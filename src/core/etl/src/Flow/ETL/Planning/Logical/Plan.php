<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Logical;

use Flow\ETL\Planning\Physical\Planner;

/**
 * Frozen: later verbs on the frame do not reach it.
 */
final readonly class Plan
{
    private function __construct(
        public LogicalPlan $logical,
        public Planner $planner,
    ) {}

    public static function of(LogicalPlan $logical, Planner $planner): self
    {
        return new self($logical, $planner);
    }

    /**
     * A subtree several consumers share is printed once and referenced by its number afterwards. The optimized stage
     * is the plan the configured optimizer hands to the planner; a joined or read frame is part of the tree.
     */
    public function toString(Stage $stage = Stage::optimized, Format $format = Format::tree): string
    {
        if ($stage === Stage::physical) {
            return (new Explain())->physical($this->planner->plan($this->logical), $format);
        }

        return (new Explain())->of(match ($stage) {
            Stage::unoptimized => $this->logical,
            Stage::optimized => $this->planner->optimize($this->logical),
        }, $format);
    }
}
