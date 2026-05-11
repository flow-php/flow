<?php

declare(strict_types=1);

namespace Flow\Telemetry\ErrorHandler;

/**
 * Fans an error out to multiple handlers. Each child invocation is wrapped so a
 * misbehaving handler cannot prevent siblings from running.
 */
final readonly class CompositeErrorHandler implements ErrorHandler
{
    /** @var array<ErrorHandler> */
    private array $handlers;

    public function __construct(ErrorHandler ...$handlers)
    {
        $this->handlers = $handlers;
    }

    public function handle(\Throwable $error): void
    {
        foreach ($this->handlers as $handler) {
            try {
                $handler->handle($error);
            } catch (\Throwable) {
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
