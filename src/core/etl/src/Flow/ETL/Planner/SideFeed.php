<?php

declare(strict_types=1);

namespace Flow\ETL\Planner;

use Flow\ETL\ErrorHandler;
use Flow\ETL\Extractor\FeedExtractor;
use Flow\ETL\Extractor\Scan;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Pipeline\Segments;
use Flow\ETL\Pipeline\SideOffers;
use Flow\ETL\Pipeline\SideRun;
use Flow\ETL\Pipeline\SinkFeed;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Pipeline;
use Flow\ETL\Schema;

/**
 * Builds the side pipeline a sink root runs in: fed batch by batch from the spine node it attaches to.
 */
final readonly class SideFeed
{
    public function __construct(
        private Analysis $analysis,
        private FlowContext $context,
    ) {}

    /**
     * ONE pipeline, no cuts: the Executor chains every segment and processor of it like an input edge.
     *
     * @param list<Node> $nodes what runs before $tail, bottom-up
     * @param list<Loader> $tail
     * @param ErrorHandler $handler throw-only inside a transaction, the plan's own otherwise
     */
    public function of(array $nodes, array $tail, Node $host, ErrorHandler $handler, int $id): SinkFeed
    {
        $feed = new FeedExtractor($this->analysis->of($host, $this->context)->schema ?? new Schema());
        $offers = new SideOffers($handler);
        $segments = new Segments($feed);

        foreach ($nodes as $node) {
            foreach ($this->analysis->steps($node, $this->context) as $step) {
                $segments->add($step);
            }
        }

        foreach ($tail as $step) {
            $segments->add($step);
        }

        return new SinkFeed(
            $feed,
            new SideRun(
                new Pipeline($id, $segments, $this->context->withErrorHandler($offers), null, [], new Scan()),
                $this->context->config->executor(),
            ),
            $offers,
            ...$tail,
        );
    }
}
