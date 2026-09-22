<?php

declare(strict_types=1);

namespace Flow\ETL\Plan;

use function count;

/**
 * What a DataFrame verb does NOT build: the consumer on top of the plan belongs to the action that runs it.
 */
enum Trigger
{
    case rows;

    case run;

    case count;

    public function plan(Node $root, Sinks $sinks = new Sinks()): LogicalPlan
    {
        $all = $sinks->all();

        $consumers = match ($this) {
            self::rows => [new Node\Result($root), ...$all],
            self::count => [new Node\Result(new Node\Count($root)), ...$all],
            self::run => ($all[0] ?? null) instanceof Node\Write && $all[0]->children()[0] === $root
                ? $all
                : [new Node\Result($root), ...$all],
        };

        return new LogicalPlan(count($consumers) === 1 ? $consumers[0] : new Node\Outputs(...$consumers));
    }
}
