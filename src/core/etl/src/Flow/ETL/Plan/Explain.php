<?php

declare(strict_types=1);

namespace Flow\ETL\Plan;

use Flow\ETL\Plan\Explain\BoxLayout;
use Flow\ETL\Plan\Explain\FlowLayout;
use Flow\ETL\Plan\Explain\Outline;
use Flow\ETL\Plan\Explain\TreeLayout;

final readonly class Explain
{
    public function of(LogicalPlan $plan, Format $format = Format::tree): string
    {
        return (match ($format) {
            Format::tree => new TreeLayout(),
            Format::boxes => new BoxLayout(),
            Format::flow => new FlowLayout(),
            Format::declarations => new TreeLayout(declarations: true),
        })->render((new Outline())->of($plan->root));
    }
}
