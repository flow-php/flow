<?php

declare(strict_types=1);

namespace Flow\ETL\Loader;

use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\Exception\LimitReachedException;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Pipeline\TransformationStream;
use Flow\ETL\Rows;
use Flow\ETL\Transformation;
use Flow\ETL\Transformer;
use Throwable;

final class TransformerLoader implements Closure, Discardable, Loader, OverridingLoader, ReplayAware
{
    private ?TransformationStream $stream = null;

    private bool $limitReached = false;

    private ?FlowContext $runContext = null;

    public function __construct(
        private readonly Transformer|Transformation $transformer,
        private readonly Loader $loader,
    ) {}

    public function closure(FlowContext $context): void
    {
        try {
            try {
                if ($this->stream !== null && $this->stream->drivenBy($context)) {
                    $this->stream->drain();
                }
            } catch (Throwable $failure) {
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
            if ($this->runContext !== $context) {
                $this->runContext = $context;
                $this->limitReached = false;
            }

            $transformer = $this->transformer;

            if ($transformer instanceof Transformer) {
                // @mago-ignore analysis:invalid-argument,too-many-arguments,possibly-invalid-argument
                $this->loader->load($transformer->transform($rows, $context), $context);
            } else {
                if ($this->stream === null || !$this->stream->drivenBy($context)) {
                    $this->stream = new TransformationStream($transformer, $this->loader, $context);
                }

                try {
                    $this->stream->feed($rows);
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
        return [$this->loader];
    }

    public function replaySafe(): bool
    {
        return false;
    }
}
