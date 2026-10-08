<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\Plan\LogicalPlan;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\ExpandColumn;
use Flow\ETL\Plan\RequiredColumns;

use function spl_object_id;

final class ExpandColumnsContext
{
    /**
     * What each ExpandColumn of the plan carries, keyed by its column name.
     *
     * @return array<string, RequiredColumns>
     */
    public static function carries(LogicalPlan $plan): array
    {
        $carries = [];
        $seen = [];
        $pending = [$plan->root];

        while ($pending !== []) {
            $node = array_pop($pending);

            if (array_key_exists(spl_object_id($node), $seen)) {
                continue;
            }

            $seen[spl_object_id($node)] = true;

            if ($node instanceof ExpandColumn) {
                $carries[$node->name()] = $node->carries;
            }

            foreach ($node instanceof Node\JoinsFrame ? [$node->children()[0]] : $node->children() as $child) {
                $pending[] = $child;
            }
        }

        return $carries;
    }
}
