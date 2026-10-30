<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Mother;

use Flow\ETL\IO\Extractor\FeedExtractor;
use Flow\ETL\IO\Loader;
use Flow\ETL\Operators\Processor;
use Flow\ETL\Operators\Transformer;
use Flow\ETL\Planning\Physical\Planner;
use Flow\ETL\Runtime\Executor\BranchFiber;
use Flow\ETL\Runtime\Executor\BranchLoader;
use Flow\ETL\Runtime\Executor\Executor;
use Flow\ETL\Runtime\Executor\OfferedFailure;
use Flow\ETL\Runtime\Executor\StepRunner;
use Flow\ETL\Runtime\FlowContext;
use Flow\ETL\Values\Rows;

use function array_values;
use function count;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;

final class BranchLoaderMother
{
    public static function batch(int $id = 1): Rows
    {
        return array_to_rows([['id' => $id]], schema(int_schema('id')));
    }

    public static function feed(): FeedExtractor
    {
        return new FeedExtractor(schema(int_schema('id')));
    }

    /**
     * The run over a branch pipeline of $steps, fed by $feed, under $context.
     */
    public static function run(
        FeedExtractor $feed,
        FlowContext $context,
        Transformer|Loader|Processor ...$steps,
    ): BranchFiber {
        $pipelines = Planner::cut(null, array_values($steps), 0);

        return new BranchFiber($pipelines[count($pipelines) - 1], new Executor(), $context, $feed, new StepRunner());
    }

    /**
     * The wiring the planner builds for one branch: $before, then the branch's own loader, under a context whose
     * handler is $offers.
     */
    public static function branchLoader(
        OfferedFailure $offers,
        Loader $loader,
        Transformer|Processor ...$before,
    ): BranchLoader {
        $feed = self::feed();

        return new BranchLoader(
            $feed,
            self::run($feed, NodeMother::context()->withErrorHandler($offers), ...[...$before, $loader]),
            $offers,
            $loader,
        );
    }
}
