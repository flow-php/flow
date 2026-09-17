<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Mother;

use Flow\ETL\Executor;
use Flow\ETL\Executor\Pipeline;
use Flow\ETL\Executor\Segments;
use Flow\ETL\Executor\SinkFeed;
use Flow\ETL\Executor\SinkOffers;
use Flow\ETL\Executor\SinkRun;
use Flow\ETL\Extractor\FeedExtractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Processor;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;

final class SinkFeedMother
{
    public static function batch(int $id = 1): Rows
    {
        return rows(schema(int_schema('id')), row(['id' => $id]));
    }

    public static function feed(): FeedExtractor
    {
        return new FeedExtractor(schema(int_schema('id')));
    }

    public static function pipeline(
        FeedExtractor $feed,
        FlowContext $context,
        Transformer|Loader|Processor ...$steps,
    ): Pipeline {
        $segments = new Segments($feed);

        foreach ($steps as $step) {
            $segments->add($step);
        }

        return new Pipeline(0, $segments, $context);
    }

    /**
     * The wiring the planner builds for one non-bare sink: $before, then the sink's own loader, under a context whose
     * handler is $offers.
     */
    public static function sinkFeed(SinkOffers $offers, Loader $loader, Transformer|Processor ...$before): SinkFeed
    {
        $feed = self::feed();

        return new SinkFeed(
            $feed,
            new SinkRun(
                self::pipeline($feed, NodeMother::context()->withErrorHandler($offers), ...[...$before, $loader]),
                new Executor(),
            ),
            $offers,
            $loader,
        );
    }
}
