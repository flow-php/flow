<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Mother;

use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Sinks;
use Flow\ETL\Planner;
use Flow\ETL\Planner\PlannedNodes;
use Flow\ETL\Planner\SinkAttachment;
use SplObjectStorage;

final class SinkAttachmentMother
{
    /**
     * An attachment over a planned spine and planned sinks, with the spine's node set as attach() expects it.
     *
     * @return array{SinkAttachment, SplObjectStorage<Node, Node>}
     */
    public static function over(Sinks $sinks, Node ...$spine): array
    {
        $planned = new PlannedNodes();

        foreach ($sinks as $sink) {
            (new Planner())->node($sink, NodeMother::context(), $planned);
        }

        /** @var SplObjectStorage<Node, Node> $onSpine */
        $onSpine = new SplObjectStorage();

        foreach ($spine as $node) {
            (new Planner())->node($node, NodeMother::context(), $planned);
            $onSpine[$node] = $node;
        }

        return [new SinkAttachment($planned, NodeMother::context()), $onSpine];
    }
}
