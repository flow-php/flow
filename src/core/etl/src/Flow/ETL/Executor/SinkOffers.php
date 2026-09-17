<?php

declare(strict_types=1);

namespace Flow\ETL\Executor;

use Flow\ETL\ErrorHandler;
use Flow\ETL\ErrorHandler\ExtractionAction;
use Flow\ETL\ErrorHandler\ExtractionError;
use Flow\ETL\ErrorHandler\LoadingAction;
use Flow\ETL\ErrorHandler\LoadingError;
use Flow\ETL\ErrorHandler\TransformationAction;
use Flow\ETL\ErrorHandler\TransformationError;
use Throwable;

/**
 * The sink pipeline's handler - it records what it was offered and leaves the decision to $handler.
 */
final class SinkOffers implements ErrorHandler
{
    private ?Throwable $last = null;

    public function __construct(
        private readonly ErrorHandler $handler,
    ) {}

    public function onExtraction(ExtractionError $error): ExtractionAction
    {
        $this->last = $error->cause;

        return $this->handler->onExtraction($error);
    }

    public function onTransformation(TransformationError $error): TransformationAction
    {
        $this->last = $error->cause;

        return $this->handler->onTransformation($error);
    }

    public function onLoading(LoadingError $error): LoadingAction
    {
        $this->last = $error->cause;

        return $this->handler->onLoading($error);
    }

    /**
     * By identity: Segment hands the handler the failure and rethrows the same instance.
     */
    public function offered(Throwable $failure): bool
    {
        return $this->last === $failure;
    }

    public function forget(): void
    {
        $this->last = null;
    }
}
