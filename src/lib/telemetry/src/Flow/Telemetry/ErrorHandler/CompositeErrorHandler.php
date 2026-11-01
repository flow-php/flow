<?php

declare(strict_types=1);

namespace Flow\Telemetry\ErrorHandler;

use Throwable;

/**
 * A throwing child never stops its siblings.
 */
final readonly class CompositeErrorHandler implements ErrorHandler
{
    /** @var array<ErrorHandler> */
    private array $handlers;

    public function __construct(ErrorHandler ...$handlers)
    {
        $this->handlers = $handlers;
    }

    public function handle(Throwable $error): void
    {
        foreach ($this->handlers as $handler) {
            try {
                $handler->handle($error);
            } catch (Throwable) {
            }
        }
    }

    /**
     * @return array<ErrorHandler>
     */
    public function handlers(): array
    {
        return $this->handlers;
    }
}
