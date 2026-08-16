<?php

declare(strict_types=1);

namespace Flow\ETL\Loader;

use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\Exception\LimitReachedException;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Pipeline\TransformationDrive;
use Flow\ETL\Rows;
use Flow\ETL\Transformation;
use Flow\ETL\Transformer;
use Throwable;

final class TransformerLoader implements Closure, Loader, OverridingLoader, ReplayAware
{
    private ?TransformationDrive $drive = null;

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
                // A drive left behind by a dead earlier run must not be drained here - it would commit that run's
                // buffered rows under the dead run's context. The finally below discards it, exactly as load() does.
                if ($this->drive !== null && $this->drive->drivenBy($context)) {
                    $this->drive->drain();
                }
            } catch (Throwable $failure) {
                // A drain failure never reached load(), so this is the only place the ErrorHandler can rule on it.
                // Declining to propagate means the run continues, which on an outer frame leaves the loader closed -
                // so fall through to the wrapped closure() instead of rethrowing.
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
            // Deliberately NOT folded into TransformationDrive::drivenBy(): drivenBy() answers "was this DRIVE built
            // for this run?" and decides rebuild; this field answers "is this a new RUN?" and decides the limit-dedup
            // reset. A drive dropped after a mid-run failure leaves the run unchanged - the rebuild must not re-arm
            // limit reporting, or one logical limit event would be reported once per rebuild instead of once per run.
            if ($this->runContext !== $context) {
                $this->runContext = $context;
                $this->limitReached = false;
            }

            $transformer = $this->transformer;

            if ($transformer instanceof Transformer) {
                // @mago-ignore analysis:invalid-argument,too-many-arguments,possibly-invalid-argument
                $this->loader->load($transformer->transform($rows, $context), $context);
            } else {
                // A run that dies before the closure loop never reaches closure()'s reset, so a loader reused by a
                // later run would resume the dead run's fiber and feed its rows through the dead run's context.
                if ($this->drive === null || !$this->drive->drivenBy($context)) {
                    $this->drive = new TransformationDrive($transformer, $this->loader, $context);
                }

                try {
                    $this->drive->feed($rows);
                } catch (Throwable $failure) {
                    // A dead fiber cannot resume - the nested pipeline's state died with it. Drop the drive so a
                    // batch offered after this one arrives at a fresh one, exactly as it would reach a fresh loader
                    // call on an outer frame. Whether the run continues at all is Segment's ruling, via the
                    // ErrorHandler; this loader only makes sure it is still usable if it does.
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
        return [$this->loader];
    }

    public function replaySafe(): bool
    {
        // Both branches: the Transformation branch owns a stream-spanning drive; the raw-Transformer branch holds a
        // long-lived Transformer whose state cannot be rewound and whose statefulness is undetectable from outside.
        return false;
    }
}
