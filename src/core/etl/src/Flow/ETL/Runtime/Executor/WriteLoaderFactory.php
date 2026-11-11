<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\Executor;

use Flow\ETL\Foundation\Progress;
use Flow\ETL\IO\Extractor\FeedExtractor;
use Flow\ETL\IO\Loader;
use Flow\ETL\Operators\Processor;
use Flow\ETL\Operators\Transformer;
use Flow\ETL\Planning\Physical\BranchPipeline;
use Flow\ETL\Planning\Physical\JoinStep;
use Flow\ETL\Planning\Physical\TransactionGroup;
use Flow\ETL\Runtime\ErrorHandler;
use Flow\ETL\Runtime\FlowContext;
use SplObjectStorage;
use WeakMap;

/**
 * One instance per run, never shared; builds each branch step once.
 */
final class WriteLoaderFactory
{
    /**
     * @var WeakMap<BranchPipeline|TransactionGroup, NestedLoader>
     */
    private WeakMap $built;

    public function __construct()
    {
        $this->built = new WeakMap();
    }

    /**
     * The step that feeds $branch's pipeline. Its writes report through the feed's offers, so a failure they
     * offered is not offered again when it escapes the feed.
     */
    public function feed(BranchPipeline $branch, ?ErrorHandler $handler, FlowContext $context): BranchLoader
    {
        $offers = new OfferedFailure($handler);
        $feed = new FeedExtractor($branch->input);
        $writes = new self();

        return new BranchLoader(
            $feed,
            new BranchFiber(
                $branch->pipeline,
                $context->config->executor(),
                $context->withErrorHandler($offers),
                $feed,
                new StepRunner(writes: $writes),
            ),
            $offers,
            ...array_map(
                static fn(Loader|BranchPipeline|TransactionGroup $write): Loader|NestedLoader => $writes->write(
                    $write,
                    $offers,
                    $context,
                    $branch->pipeline->steps->stages,
                    $branch->pipeline->steps->writes,
                ),
                $branch->writes(),
            ),
        );
    }

    /**
     * One transaction around its writes; a failure of a write in it rolls the transaction back, so none is offered.
     *
     * @param null|SplObjectStorage<Loader|Transformer, Progress> $stages
     * @param null|SplObjectStorage<Loader, Progress> $writes
     */
    public function transactional(
        TransactionGroup $branch,
        FlowContext $context,
        ?SplObjectStorage $stages = null,
        ?SplObjectStorage $writes = null,
    ): TransactionLoader {
        return new TransactionLoader(
            $branch->transaction,
            array_map(fn(Loader|BranchPipeline $child): Loader|NestedLoader => $child instanceof BranchPipeline
                ? $this->feed($child, null, $context)
                : $child, $branch->children),
            $context,
            $stages,
            $writes,
        );
    }

    /**
     * $write as the runtime runs it: a branch becomes the step that feeds it, a transaction the step that wraps its
     * writes, the same one each time; a loader is itself.
     *
     * @param null|SplObjectStorage<Loader|Transformer, Progress> $stages
     * @param null|SplObjectStorage<Loader, Progress> $writes
     */
    public function write(
        Loader|BranchPipeline|TransactionGroup $write,
        ErrorHandler $handler,
        FlowContext $context,
        ?SplObjectStorage $stages = null,
        ?SplObjectStorage $writes = null,
    ): Loader|NestedLoader {
        return match (true) {
            $write instanceof BranchPipeline => $this->built[$write] ??= $this->feed($write, $handler, $context),
            $write instanceof TransactionGroup => $this->built[$write] ??= $this->transactional(
                $write,
                $context,
                $stages,
                $writes,
            ),
            default => $write,
        };
    }

    /**
     * @param null|SplObjectStorage<Loader|Transformer, Progress> $stages
     * @param null|SplObjectStorage<Loader, Progress> $writes
     */
    public function step(
        JoinStep|Loader|Processor|BranchPipeline|TransactionGroup|Transformer $step,
        ErrorHandler $handler,
        FlowContext $context,
        ?SplObjectStorage $stages = null,
        ?SplObjectStorage $writes = null,
    ): JoinStep|Loader|Processor|Transformer|NestedLoader {
        return $step instanceof JoinStep || $step instanceof Processor || $step instanceof Transformer
            ? $step
            : $this->write($step, $handler, $context, $stages, $writes);
    }
}
