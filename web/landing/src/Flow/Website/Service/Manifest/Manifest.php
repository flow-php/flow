<?php

declare(strict_types=1);

namespace Flow\Website\Service\Manifest;

final class Manifest
{
    /** @var null|array<string, array<string, mixed>> */
    private ?array $byName = null;

    public function __construct(private readonly string $manifestPath)
    {
    }

    /**
     * @return array<string, array<string, mixed>>
     */
    public function all() : array
    {
        return $this->load();
    }

    /**
     * @return null|array<string, mixed>
     */
    public function byName(string $packageName) : ?array
    {
        return $this->load()[$packageName] ?? null;
    }

    /**
     * @return array<string, array<string, mixed>>
     */
    private function load() : array
    {
        if ($this->byName !== null) {
            return $this->byName;
        }

        if (!is_file($this->manifestPath)) {
            throw new \RuntimeException(sprintf('Flow manifest not found at "%s".', $this->manifestPath));
        }

        $raw = file_get_contents($this->manifestPath);

        if ($raw === false) {
            throw new \RuntimeException(sprintf('Failed to read Flow manifest at "%s".', $this->manifestPath));
        }

        /** @var array{packages?: list<array<string, mixed>>} $decoded */
        $decoded = json_decode($raw, true, flags: JSON_THROW_ON_ERROR);

        $byName = [];

        foreach ($decoded['packages'] ?? [] as $entry) {
            if (!isset($entry['name']) || !is_string($entry['name'])) {
                continue;
            }
            $byName[$entry['name']] = $entry;
        }

        return $this->byName = $byName;
    }
}
