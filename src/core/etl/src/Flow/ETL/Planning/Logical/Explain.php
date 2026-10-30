<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Logical;

use Flow\ETL\Planning\Explain\BoxLayout;
use Flow\ETL\Planning\Explain\Entry;
use Flow\ETL\Planning\Explain\FlowLayout;
use Flow\ETL\Planning\Explain\LogicalOutline;
use Flow\ETL\Planning\Explain\PhysicalOutline;
use Flow\ETL\Planning\Explain\TreeLayout;
use Flow\ETL\Planning\Physical\PhysicalPlan;

final readonly class Explain
{
    public function of(LogicalPlan $plan, Format $format = Format::tree): string
    {
        return $this->render(
            (new LogicalOutline(declarations: $format === Format::declarations))->of($plan->root),
            $format,
        );
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
