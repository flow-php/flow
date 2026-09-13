<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP;

use DateTimeImmutable;
use Flow\Filesystem\Exception\RuntimeException;
use phpseclib3\Net\SFTP;
use stdClass;

use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_optional;
use function is_array;
use function krsort;

final readonly class DirectoryListing
{
    public function __construct(
        private SFTP $sftp,
    ) {}

    public function read(string $directory): DirectoryEntries
    {
        /** @var array<array-key, array<array-key, mixed>|stdClass>|false $rawList */
        $rawList = $this->sftp->rawlist($directory, true);

        if (!is_array($rawList)) {
            $this->sftp->isConnected() && $this->sftp->isAuthenticated()
                || throw new RuntimeException('SFTP session is no longer usable, cannot list ' . $directory);

            return new DirectoryEntries();
        }

        return self::fromRawList($directory, $rawList);
    }

    /**
     * @param array<array-key, array<array-key, mixed>|stdClass> $rawList
     */
    private static function fromRawList(string $directory, array $rawList): DirectoryEntries
    {
        krsort($rawList, SORT_STRING);

        $entries = new DirectoryEntries();

        foreach ($rawList as $name => $value) {
            if ($name === '.' || $name === '..') {
                continue;
            }

            $path = $directory . '/' . $name;

            if (is_array($value)) {
                /** @var array<array-key, array<array-key, mixed>|stdClass> $value */
                $entries = new DirectoryEntries(
                    DirectoryEntry::subdirectory($path, self::fromRawList($path, $value)),
                    $entries,
                );

                continue;
            }

            $entries = new DirectoryEntries(
                DirectoryEntry::file(
                    $path,
                    type_optional(type_integer())->assert($value->size ?? null),
                    isset($value->mtime) ? new DateTimeImmutable('@' . type_integer()->assert($value->mtime)) : null,
                ),
                $entries,
            );
        }

        return $entries;
    }
}
