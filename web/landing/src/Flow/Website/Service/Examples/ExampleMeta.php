<?php

declare(strict_types=1);

namespace Flow\Website\Service\Examples;

use Symfony\Component\Yaml\Yaml;

use function array_key_exists;
use function file_exists;
use function file_get_contents;
use function implode;
use function in_array;
use function is_array;
use function is_string;
use function trim;

final readonly class ExampleMeta
{
    private const TOP_LEVEL = ['priority', 'hidden', 'run'];

    private const RUN = ['skip'];

    private const DEFAULT_PRIORITY = 99;

    public function __construct(
        public int $priority,
        public bool $hidden,
        public ?string $skipReason,
    ) {}

    /**
     * @throws InvalidExampleMetaException
     */
    public static function fromDirectory(string $directory): self
    {
        $path = $directory . '/_meta.yaml';

        if (!file_exists($path)) {
            return new self(self::DEFAULT_PRIORITY, false, null);
        }

        /** @var mixed $meta */
        $meta = Yaml::parse((string) file_get_contents($path));

        if ($meta === null) {
            return new self(self::DEFAULT_PRIORITY, false, null);
        }

        if (!is_array($meta)) {
            throw InvalidExampleMetaException::badValue($path, '<root>', 'a map');
        }

        foreach ($meta as $key => $_value) {
            if (!is_string($key) || !in_array($key, self::TOP_LEVEL, true)) {
                throw InvalidExampleMetaException::unknownKey($path, (string) $key, implode(', ', self::TOP_LEVEL));
            }
        }

        return new self(self::priorityOf($path, $meta), self::hiddenOf($path, $meta), self::skipReasonOf($path, $meta));
    }

    /**
     * @param array<array-key, mixed> $meta
     */
    private static function hiddenOf(string $path, array $meta): bool
    {
        /** @var mixed $hidden */
        $hidden = $meta['hidden'] ?? false;

        if (!is_bool($hidden)) {
            throw InvalidExampleMetaException::badValue($path, 'hidden', 'a boolean');
        }

        return $hidden;
    }

    /**
     * 99 is the fallback for an example with no _meta.yaml, so leaving it there means the example
     * sorts alphabetically by accident rather than in the order a reader should meet it.
     *
     * @param array<array-key, mixed> $meta
     */
    private static function priorityOf(string $path, array $meta): int
    {
        /** @var mixed $priority */
        $priority = $meta['priority'] ?? self::DEFAULT_PRIORITY;

        if (!is_int($priority) || $priority === self::DEFAULT_PRIORITY) {
            throw InvalidExampleMetaException::badValue($path, 'priority', 'an integer other than 99');
        }

        return $priority;
    }

    /**
     * @param array<array-key, mixed> $meta
     */
    private static function skipReasonOf(string $path, array $meta): ?string
    {
        if (!array_key_exists('run', $meta)) {
            return null;
        }

        /** @var mixed $run */
        $run = $meta['run'];

        if (!is_array($run)) {
            throw InvalidExampleMetaException::badValue($path, 'run', 'a map');
        }

        foreach ($run as $key => $_value) {
            if (!is_string($key) || !in_array($key, self::RUN, true)) {
                throw InvalidExampleMetaException::unknownKey($path, 'run.' . (string) $key, implode(', ', self::RUN));
            }
        }

        if (!array_key_exists('skip', $run)) {
            return null;
        }

        /** @var mixed $skip */
        $skip = $run['skip'];

        if (!is_string($skip) || trim($skip) === '') {
            throw InvalidExampleMetaException::badValue($path, 'run.skip', 'a non-empty string');
        }

        return $skip;
    }
}
