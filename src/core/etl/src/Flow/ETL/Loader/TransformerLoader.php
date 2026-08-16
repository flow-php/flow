<?php

declare(strict_types=1);

namespace Flow\ETL\Loader;

use Fiber;
use FiberError;
use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Exception\LimitReachedException;
use Flow\ETL\Extractor\FeedExtractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Rows;
use Flow\ETL\Transformation;
use Flow\ETL\Transformer;
use Throwable;

use function Flow\ETL\DSL\df;

final class TransformerLoader implements Closure, Loader, OverridingLoader
{
    private bool $limitReached = false;

    private ?FlowContext $transformationContext = null;

    private ?Fiber $transformationFiber = null;

    private FeedExtractor $transformationRows;

    public function __construct(
        private readonly Transformer|Transformation $transformer,
        private readonly Loader $loader,
    ) {
        $this->transformationRows = new FeedExtractor();
    }

    public function closure(FlowContext $context): void
    {
        try {
            if ($this->transformationFiber?->isSuspended()) {
                $this->transformationRows->finish();

                try {
                    $this->transformationFiber->resume();
                } catch (Throwable $failure) {
                    // A drain failure never reached load(), so this is the only place the ErrorHandler can rule on
                    // it. Declining to propagate means the run continues, which on an outer frame leaves the loader
                    // closed - so fall through to the wrapped closure() instead of rethrowing.
                    if ($context->errorHandler()->throw($failure, new Rows())) {
                        throw $failure;
                    }
                }
            }

            if ($this->loader instanceof Closure) {
                $this->loader->closure($context);
            }
        } finally {
            $this->transformationContext = null;
            $this->transformationFiber = null;
            $this->transformationRows = new FeedExtractor();
            $this->limitReached = false;
        }
    }

    public function load(Rows $rows, FlowContext $context): void
    {
        $context->telemetry()->loadingStarted($this);

        try {
            $transformer = $this->transformer;

            if ($transformer instanceof Transformer) {
                // @mago-ignore analysis:invalid-argument,too-many-arguments,possibly-invalid-argument
                $this->loader->load($transformer->transform($rows, $context), $context);
            } else {
                // A run that dies before the closure loop never reaches closure()'s reset, so a loader reused by a
                // later run would resume the dead run's fiber and feed its rows through the dead run's context.
                if ($this->transformationContext !== $context) {
                    $this->transformationContext = $context;
                    $this->transformationFiber = null;
                    $this->transformationRows = new FeedExtractor();
                    $this->limitReached = false;
                }

                if ($this->transformationFiber === null) {
                    try {
                        $frame = $transformer->transform(df($context->config)->from($this->transformationRows));
                        // @mago-ignore analysis:avoid-catching-error
                        // Nothing is hidden - the FiberError is rethrown as the previous exception. It only ever
                        // means the Transformation triggered the frame, which no message from the engine explains.
                    } catch (FiberError $error) {
                        throw new InvalidLogicException(
                            'A Transformation given to to_transformation() must only build the DataFrame, not '
                            . 'trigger it - count(), fetch(), schema() and the other trigger methods read from a '
                            . 'source that only exists while the nested pipeline runs.',
                            previous: $error,
                        );
                    }

                    $this->transformationFiber = new Fiber(function () use ($frame, $context): void {
                        foreach ($frame->get() as $transformedRows) {
                            $this->loader->load($transformedRows, $context);
                        }
                    });
                }

                if (!$this->transformationFiber->isTerminated()) {
                    $this->transformationRows->feed($rows);

                    try {
                        $this->transformationFiber->isStarted()
                            ? $this->transformationFiber->resume()
                            : $this->transformationFiber->start();
                    } catch (Throwable $failure) {
                        // A dead fiber cannot resume - the nested pipeline's state died with it. Drop it so a batch
                        // offered after this one arrives at a fresh drive, exactly as it would reach a fresh loader
                        // call on an outer frame. Whether the run continues at all is Segment's ruling, via the
                        // ErrorHandler; this loader only makes sure it is still usable if it does.
                        $this->transformationFiber = null;
                        $this->transformationRows = new FeedExtractor();

                        throw $failure;
                    }
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
}
