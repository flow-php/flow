<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\Executor;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Runtime\ErrorHandler;
use Flow\ETL\Runtime\ErrorHandler\ExtractionAction;
use Flow\ETL\Runtime\ErrorHandler\ExtractionError;
use Flow\ETL\Runtime\ErrorHandler\LoadingAction;
use Flow\ETL\Runtime\ErrorHandler\LoadingError;
use Flow\ETL\Runtime\ErrorHandler\TransformationAction;
use Flow\ETL\Runtime\ErrorHandler\TransformationError;
use SensitiveParameter;
use Throwable;

/**
 * The branch pipeline's handler - it remembers the last loading or transformation error it was offered and leaves every
 * decision to $handler.
 */
final class OfferedFailure implements ErrorHandler
{
    private LoadingError|TransformationError|null $last = null;

    /**
     * @param null|ErrorHandler $handler null when nothing inside the branch decides: every error propagates, and the
     *                                   caller decides it through undecided()
     */
    public function __construct(
        private readonly ?ErrorHandler $handler,
    ) {}

    public function onExtraction(ExtractionError $error): ExtractionAction
    {
        return $this->handler?->onExtraction($error) ?? ExtractionAction::propagate;
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        throw InvalidArgumentException::because('%s holds live state and is never described', self::class);
    }

    public function describe(): ?Description
    {
        return null;
    }

    public function onTransformation(TransformationError $error): TransformationAction
    {
        $this->last = $error;

        return $this->handler?->onTransformation($error) ?? TransformationAction::propagate;
    }

    public function onLoading(LoadingError $error): LoadingAction
    {
        $this->last = $error;

        return $this->handler?->onLoading($error) ?? LoadingAction::propagate;
    }

    /**
     * The error $failure was offered in, by identity, when no handler decided it: PipelineSteps hands the handler the
     * failure and rethrows the same instance. null when a handler decided it or no user step raised it.
     */
    public function undecided(Throwable $failure): LoadingError|TransformationError|null
    {
        return $this->handler === null && $this->last?->cause === $failure ? $this->last : null;
    }

    public function forget(): void
    {
        $this->last = null;
    }
}
