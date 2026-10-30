<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\Executor;

use Flow\ETL\IO\Extractor\FeedExtractor;
use Flow\ETL\IO\Loader;
use Flow\ETL\Operators\Processor;
use Flow\ETL\Operators\Transformer;
use Flow\ETL\Planning\Physical\BranchPipeline;
use Flow\ETL\Planning\Physical\JoinStep;
use Flow\ETL\Planning\Physical\TransactionGroup;
use Flow\ETL\Runtime\ErrorHandler;
use Flow\ETL\Runtime\FlowContext;
use WeakMap;

/**
 * Builds the runtime step of each branch and transaction of one run, once: a fiber restarted over the same pipeline
 * runs the steps a BranchLoader forwards its discard() to. Never shared between runs.
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
                ),
                $branch->writes(),
            ),
        );
    }

    /**
     * One transaction around its writes; a failure of a write in it rolls the transaction back, so none is offered.
     */
    public function transactional(TransactionGroup $branch, FlowContext $context): TransactionLoader
    {
        return new TransactionLoader(
            $branch->transaction,
            array_map(fn(Loader|BranchPipeline $child): Loader|NestedLoader => $child instanceof BranchPipeline
                ? $this->feed($child, null, $context)
                : $child, $branch->children),
            $context,
        );
    }

    /**
     * $write as the runtime runs it: a branch becomes the step that feeds it, a transaction the step that wraps its
     * writes, the same one each time; a loader is itself.
     */
    public function write(
        Loader|BranchPipeline|TransactionGroup $write,
        ErrorHandler $handler,
        FlowContext $context,
    ): Loader|NestedLoader {
        return match (true) {
            $write instanceof BranchPipeline => $this->built[$write] ??= $this->feed($write, $handler, $context),
            $write instanceof TransactionGroup => $this->built[$write] ??= $this->transactional($write, $context),
            default => $write,
        };
    }

    public function step(
        JoinStep|Loader|Processor|BranchPipeline|TransactionGroup|Transformer $step,
        ErrorHandler $handler,
        FlowContext $context,
    ): JoinStep|Loader|Processor|Transformer|NestedLoader {
        return $step instanceof JoinStep || $step instanceof Processor || $step instanceof Transformer
            ? $step
            : $this->write($step, $handler, $context);
    }
}
