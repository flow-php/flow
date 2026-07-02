<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger;

use Symfony\Component\Messenger\Stamp\StampInterface;

use function strtolower;

/**
 * Messenger stamp holding propagated telemetry context.
 *
 * This stamp carries trace context information (such as traceparent and tracestate headers)
 * across message dispatch/consume boundaries, enabling distributed tracing through
 * asynchronous message queues.
 */
final readonly class TelemetryStamp implements StampInterface
{
    /**
     * @param array<string, string> $context The propagated context headers
     */
    public function __construct(
        private array $context = [],
    ) {}

    /**
     * Get all context entries.
     *
     * @return array<string, string>
     */
    public function all(): array
    {
        return $this->context;
    }

    /**
     * Getter exposing the context to the Symfony Serializer's ObjectNormalizer.
     *
     * The messenger transport's default `symfony_serializer` normalizes stamps via getter methods;
     * without a getter named after the `$context` constructor argument the array is dropped, which
     * silently strips the propagated traceparent when a message crosses an async transport.
     *
     * @return array<string, string>
     */
    public function getContext(): array
    {
        return $this->context;
    }

    /**
     * Get a context value by key.
     *
     * Key lookup is case-insensitive for HTTP header compatibility.
     */
    public function get(string $key): ?string
    {
        $lowercaseKey = strtolower($key);

        foreach ($this->context as $contextKey => $value) {
            if (strtolower($contextKey) === $lowercaseKey) {
                return $value;
            }
        }

        return null;
    }

    /**
     * Create a new stamp with an additional context entry.
     */
    public function with(string $key, string $value): self
    {
        $context = $this->context;
        $context[$key] = $value;

        return new self($context);
    }
}
