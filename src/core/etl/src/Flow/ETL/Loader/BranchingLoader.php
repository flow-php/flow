<?php

declare(strict_types=1);

namespace Flow\ETL\Loader;

use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\Exception\LimitReachedException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Loader;
use Flow\ETL\Pipeline\TransformationDrive;
use Flow\ETL\Rows;
use Flow\ETL\Transformation;
use Flow\ETL\Transformer\ScalarFunctionFilterTransformer;
use Throwable;

final class BranchingLoader implements Closure, Loader, OverridingLoader, ReplayAware
{
    private ?TransformationDrive $drive = null;

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
                // A drive left behind by a dead earlier run must not be drained here - it would commit that run's
                // buffered rows under the dead run's context. The finally below discards it, exactly as load() does.
                if ($this->drive !== null && $this->drive->drivenBy($context)) {
                    $this->drive->drain();
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
            $this->drive = null;
            $this->limitReached = false;
            $this->runContext = null;
        }
    }

    public function load(Rows $rows, FlowContext $context): void
    {
        $context->telemetry()->loadingStarted($this);

        try {
            // Same ruling split as TransformerLoader::load(): drivenBy() decides rebuild, this field decides the
            // limit-dedup reset - a mid-run drive rebuild must not re-arm limit reporting.
            if ($this->runContext !== $context) {
                $this->runContext = $context;
                $this->limitReached = false;
            }

            $branchRows = (new ScalarFunctionFilterTransformer($this->condition))->transform($rows, $context);

            if ($this->transformation === null) {
                $this->loader->load($branchRows, $context);
            } else {
                if ($this->drive === null || !$this->drive->drivenBy($context)) {
                    $this->drive = new TransformationDrive($this->transformation, $this->loader, $context);
                }

                try {
                    $this->drive->feed($branchRows);
                } catch (Throwable $failure) {
                    $this->drive = null;

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
