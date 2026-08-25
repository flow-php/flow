<?php

declare(strict_types=1);

namespace Flow\ETL\Loader;

use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\Exception\LimitReachedException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Loader;
use Flow\ETL\Pipeline\TransformationStream;
use Flow\ETL\Rows;
use Flow\ETL\Transformation;
use Flow\ETL\Transformer\ScalarFunctionFilterTransformer;
use Throwable;

final class BranchingLoader implements Closure, Discardable, Loader, OverridingLoader, ReplayAware
{
    private ?TransformationStream $stream = null;

    private bool $limitReached = false;

    private ?FlowContext $runContext = null;

    public function __construct(
        private readonly ScalarFunction $condition,
        private readonly Loader $loader,
        private ?Transformation $transformation = null,
    ) {}

    public function closure(FlowContext $context): void
    {
        try {
            try {
                // A stream left behind by a dead earlier run must not be drained here - it would commit that run's
                // buffered rows under the dead run's context. The finally below discards it, exactly as load() does.
                if ($this->stream !== null && $this->stream->drivenBy($context)) {
                    $this->stream->drain();
                }
            } catch (Throwable $failure) {
                // Same ruling as TransformerLoader::closure(): a drain failure never reached load(), so the
                // ErrorHandler rules here; declining means the run continues and the loader must still close.
                if ($context->errorHandler()->throw($failure, new Rows())) {
                    throw $failure;
                }
            }

            if ($this->loader instanceof Closure) {
                $this->loader->closure($context);
            }
        } finally {
            $this->stream = null;
            $this->limitReached = false;
            $this->runContext = null;
        }
    }

    public function discard(FlowContext $context): void
    {
        // The stream is never drained here - draining would commit the dead run's buffered rows. The wrapped loader
        // is discarded by the pipeline, which walks the whole loader tree.
        $this->stream = null;
        $this->limitReached = false;
        $this->runContext = null;
    }

    public function load(Rows $rows, FlowContext $context): void
    {
        $context->telemetry()->loadingStarted($this);

        try {
            // Same ruling split as TransformerLoader::load(): drivenBy() decides rebuild, this field decides the
            // limit-dedup reset - a mid-run stream rebuild must not re-arm limit reporting.
            if ($this->runContext !== $context) {
                $this->runContext = $context;
                $this->limitReached = false;
            }

            $branchRows = (new ScalarFunctionFilterTransformer($this->condition))->transform($rows, $context);

            if ($this->transformation === null) {
                $this->loader->load($branchRows, $context);
            } else {
                if ($this->stream === null || !$this->stream->drivenBy($context)) {
                    $this->stream = new TransformationStream($this->transformation, $this->loader, $context);
                }

                try {
                    $this->stream->feed($branchRows);
                } catch (Throwable $failure) {
                    $this->stream = null;

                    throw $failure;
                }
            }

            $context->telemetry()->loadingCompleted($this, [TelemetryAttributes::ATTR_LOADING_ROWS => $rows->count()]);
        } catch (LimitReachedException $e) {
            if (!$this->limitReached) {
                $this->limitReached = true;
                $context->telemetry()->limitReached(['limit' => $e->limit]);
            }

            $context->telemetry()->loadingCompleted($this, [TelemetryAttributes::ATTR_LOADING_ROWS => 0]);
        } catch (Throwable $e) {
            $context->telemetry()->loadingFailed($this, $e);

            throw $e;
        }
    }

    public function loaders(): array
    {
        return [
            $this->loader,
        ];
    }

    public function replaySafe(): bool
    {
        // No transformation: a fresh stateless ScalarFunctionFilterTransformer per call - replay-safe.
        return $this->transformation === null;
    }

    public function withTransformation(Transformation $transformation): self
    {
        $this->transformation = $transformation;

        return $this;
    }
}
