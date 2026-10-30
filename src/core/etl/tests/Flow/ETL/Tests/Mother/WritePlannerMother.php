<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Mother;

use Flow\ETL\Planning\Logical\Node;
use Flow\ETL\Planning\Logical\Writes;
use Flow\ETL\Planning\Physical\PlannedNodes;
use Flow\ETL\Planning\Physical\WritePlanner;
use SplObjectStorage;

use function Flow\ETL\DSL\config;

final class WritePlannerMother
{
    /**
     * A planner over a planned spine and planned writes, with the spine's node set as attach() expects it.
     *
     * @return array{WritePlanner, SplObjectStorage<Node, Node>}
     */
    public static function over(Writes $writes, Node ...$spine): array
    {
        $planned = new PlannedNodes();

        foreach ($writes as $write) {
            config()->planner()->node($write, $planned);
        }

        /** @var SplObjectStorage<Node, Node> $onSpine */
        $onSpine = new SplObjectStorage();

        foreach ($spine as $node) {
            config()->planner()->node($node, $planned);
            $onSpine[$node] = $node;
        }

        return [new WritePlanner($planned), $onSpine];
    }
}
