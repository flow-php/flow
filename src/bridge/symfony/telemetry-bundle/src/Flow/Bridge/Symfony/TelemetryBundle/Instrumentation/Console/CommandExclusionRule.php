<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Console;

use function preg_match;

final readonly class CommandExclusionRule
{
    public function __construct(
        public string $pattern,
    ) {}

    public function matches(string $commandName): bool
    {
        $result = @preg_match($this->pattern, $commandName);

        if ($result !== false) {
            return (bool) $result;
        }

        return $this->pattern === $commandName;
    }
}
