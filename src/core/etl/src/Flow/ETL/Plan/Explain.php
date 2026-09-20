<?php

declare(strict_types=1);

namespace Flow\ETL\Plan;

use Flow\ETL\Executor\PhysicalPlan;
use Flow\ETL\Plan\Explain\BoxLayout;
use Flow\ETL\Plan\Explain\Entry;
use Flow\ETL\Plan\Explain\FlowLayout;
use Flow\ETL\Plan\Explain\Outline;
use Flow\ETL\Plan\Explain\PhysicalOutline;
use Flow\ETL\Plan\Explain\TreeLayout;

final readonly class Explain
{
    public function of(LogicalPlan $plan, Format $format = Format::tree): string
    {
        return $this->render((new Outline(declarations: $format === Format::declarations))->of($plan->root), $format);
    }

    /**
     * The physical plan carries no declarations of its own, so every format but the boxes renders as a tree.
     */
    public function physical(PhysicalPlan $plan, Format $format = Format::tree): string
    {
        return $this->render((new PhysicalOutline())->of($plan), $format);
    }

    public function render(Entry $root, Format $format): string
    {
        return (match ($format) {
            Format::tree, Format::declarations => new TreeLayout(),
            Format::boxes => new BoxLayout(),
            Format::flow => new FlowLayout(),
        })->render($root);
    }
}
