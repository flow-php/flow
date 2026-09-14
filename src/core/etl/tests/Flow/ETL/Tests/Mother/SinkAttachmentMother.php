<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Mother;

use Flow\ETL\Plan\Node;
use Flow\ETL\Planner\Analysis;
use Flow\ETL\Planner\Lowerings;
use Flow\ETL\Planner\PipelineSplit;
use Flow\ETL\Planner\SinkAttachment;
use SplObjectStorage;

final class SinkAttachmentMother
{
    /**
     * An attachment over an analysed spine, with the spine's node set as attach() expects it.
     *
     * @return array{SinkAttachment, SplObjectStorage<Node, Node>}
     */
    public static function over(Node ...$spine): array
    {
        $analysis = new Analysis(Lowerings::default(), new PipelineSplit());
        /** @var SplObjectStorage<Node, Node> $onSpine */
        $onSpine = new SplObjectStorage();

        foreach ($spine as $node) {
            $analysis->of($node, NodeMother::context());
            $onSpine[$node] = $node;
        }

        return [new SinkAttachment($analysis, NodeMother::context()), $onSpine];
    }
}
