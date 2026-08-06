<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler;

use function preg_match;

final readonly class ServiceIdPatterns
{
    /**
     * @param array<string> $patterns
     */
    public function __construct(
        private array $patterns,
    ) {}

    public function matches(string $serviceId): bool
    {
        foreach ($this->patterns as $pattern) {
            $matched = @preg_match($pattern, $serviceId);

            if ($matched === false && $serviceId === $pattern) {
                return true;
            }

            if ($matched === 1) {
                return true;
            }
        }

        return false;
    }
}
