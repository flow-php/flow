<?php

declare(strict_types=1);

namespace Flow\ETL\Planner;

use Flow\ETL\Executor\Pipeline;
use Flow\ETL\Executor\Segments;
use Flow\ETL\Executor\SinkFeed;
use Flow\ETL\Executor\SinkOffers;
use Flow\ETL\Executor\SinkRun;
use Flow\ETL\Extractor\FeedExtractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Plan\Node;
use Flow\ETL\Schema;

/**
 * Builds the sink pipeline a sink root runs in: fed batch by batch from the spine node it attaches to.
 */
final readonly class SinkFeedFactory
{
    public function __construct(
        private PlannedNodes $planned,
        private FlowContext $context,
    ) {}

    /**
     * ONE pipeline, no cuts: the Executor chains every segment and processor of it like an input edge.
     *
     * @param list<Node> $nodes what runs before $tail, bottom-up
     * @param list<Loader> $tail
     * @param SinkOffers $offers the feed's handler; nested feeds report through it, so a failure they offered is not
     *                          offered again when it escapes this feed
     */
    public function of(array $nodes, array $tail, Node $host, SinkOffers $offers, int $id): SinkFeed
    {
        $feed = new FeedExtractor($this->planned->of($host)->schema ?? new Schema());
        $segments = new Segments($feed);

        foreach ($nodes as $node) {
            foreach ($this->planned->steps($node) as $step) {
                $segments->add($step);
            }
        }

        foreach ($tail as $step) {
            $segments->add($step);
        }

        return new SinkFeed(
            $feed,
            new SinkRun(
                new Pipeline($id, $segments, $this->context->withErrorHandler($offers)),
                $this->context->config->executor(),
            ),
            $offers,
            ...$tail,
        );
    }
}
