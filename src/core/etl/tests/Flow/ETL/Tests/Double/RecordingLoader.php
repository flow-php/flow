<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\ErrorHandler;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Loader\Closure;
use Flow\ETL\Loader\Discardable;
use Flow\ETL\Rows;
use Throwable;

/**
 * Logs every call in order - `load#<n>(<rows>)`, `load#<n> THROW`, `closure`, `closure THROW`, `discard` - and the
 * error handler of every context it loads under.
 */
final class RecordingLoader implements Closure, Discardable, Loader
{
    /**
     * @var list<ErrorHandler>
     */
    public array $handlers = [];

    /**
     * @var list<string>
     */
    public array $log = [];

    public int $loadsCount = 0;

    public function __construct(
        private readonly ?Throwable $loadFailure = null,
        private readonly int $failingLoad = 1,
        private readonly ?Throwable $closureFailure = null,
    ) {}

    public function closure(FlowContext $context): void
    {
        if ($this->closureFailure !== null) {
            $this->log[] = 'closure THROW';

            throw $this->closureFailure;
        }

        $this->log[] = 'closure';
    }

    public function discard(FlowContext $context): void
    {
        $this->log[] = 'discard';
    }

    public function load(Rows $rows, FlowContext $context): void
    {
        $this->loadsCount++;
        $this->handlers[] = $context->errorHandler();

        if ($this->loadFailure !== null && $this->loadsCount === $this->failingLoad) {
            $this->log[] = 'load#' . $this->loadsCount . ' THROW';

            throw $this->loadFailure;
        }

        $this->log[] = 'load#' . $this->loadsCount . '(' . $rows->count() . ')';
    }
}
