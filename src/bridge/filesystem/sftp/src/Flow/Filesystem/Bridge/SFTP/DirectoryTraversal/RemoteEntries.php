<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP\DirectoryTraversal;

use DateTimeImmutable;
use Generator;
use IteratorAggregate;
use phpseclib3\Net\SFTP;

use function array_values;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function is_array;
use function ksort;

/**
 * @implements IteratorAggregate<int, RemoteEntry>
 */
final readonly class RemoteEntries implements IteratorAggregate
{
    /**
     * @var array<int, RemoteEntry>
     */
    private array $entries;

    private function __construct(RemoteEntry ...$entries)
    {
        $this->entries = array_values($entries);
    }

    public static function in(SFTP $sftp, string $directory): ?self
    {
        /** @var array<array-key, array<array-key, mixed>>|false $rawList */
        $rawList = $sftp->rawlist($directory);

        if (!is_array($rawList)) {
            return null;
        }

        return self::parse($rawList);
    }

    /**
     * @param array<array-key, array<array-key, mixed>> $rawList
     */
    private static function parse(array $rawList): self
    {
        ksort($rawList, SORT_STRING);

        $entries = [];

        foreach ($rawList as $name => $attributes) {
            if ($name === '.' || $name === '..') {
                continue;
            }

            $entries[] = new RemoteEntry(
                self::nameOf($attributes['filename'] ?? null, $name),
                RemoteEntryType::fromRawType($attributes['type'] ?? null),
                self::sizeOf($attributes['size'] ?? null),
                self::modifiedAtOf($attributes['mtime'] ?? null),
            );
        }

        return new self(...$entries);
    }

    /**
     * @return Generator<int, RemoteEntry>
     */
    public function getIterator(): Generator
    {
        yield from $this->entries;
    }

    private static function modifiedAtOf(mixed $modificationTime): ?DateTimeImmutable
    {
        if (!type_integer()->isValid($modificationTime)) {
            return null;
        }

        return new DateTimeImmutable('@' . $modificationTime);
    }

    private static function nameOf(mixed $filename, int|string $listingKey): string
    {
        if (type_string()->isValid($filename)) {
            return $filename;
        }

        return (string) $listingKey;
    }

    private static function sizeOf(mixed $size): ?int
    {
        if (type_integer()->isValid($size)) {
            return $size;
        }

        if (type_float()->isValid($size)) {
            return (int) $size;
        }

        return null;
    }
}
