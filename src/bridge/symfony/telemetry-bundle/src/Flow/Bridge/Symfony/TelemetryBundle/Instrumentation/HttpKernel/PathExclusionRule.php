<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel;

final readonly class PathExclusionRule
{
    public function __construct(
        public string $path,
        public ?string $method = null,
    ) {
    }

    /**
     * @param array{path: string, method?: null|string} $config
     */
    public static function fromConfig(array $config) : self
    {
        return new self(
            $config['path'],
            $config['method'] ?? null,
        );
    }

    public function matches(string $path, string $method) : bool
    {
        if ($this->method !== null && \strtoupper($this->method) !== \strtoupper($method)) {
            return false;
        }

        return $this->matchesPath($path);
    }

    private function matchesPath(string $path) : bool
    {
        if (\str_starts_with($this->path, '/') && \str_ends_with($this->path, '/')) {
            return (bool) \preg_match($this->path, $path);
        }

        return $path === $this->path;
    }
}
