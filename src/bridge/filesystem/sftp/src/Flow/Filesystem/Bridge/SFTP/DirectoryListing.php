<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP;

use Flow\Filesystem\Exception\RuntimeException;
use phpseclib4\Exception\BaseException;
use phpseclib4\Net\SFTP;
use stdClass;

use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_optional;
use function is_array;

final readonly class DirectoryListing
{
    public function __construct(
        private SFTP $sftp,
    ) {}

    public function read(string $directory): DirectoryEntries
    {
        try {
            /** @var array<array-key, array<array-key, mixed>|stdClass> $rawList */
            $rawList = $this->sftp->rawlist($directory, true);
        } catch (BaseException) {
            throw new RuntimeException('SFTP session is no longer usable, cannot list ' . $directory);
        }

        return self::fromRawList($directory, $rawList);
    }

    /**
     * @param array<array-key, array<array-key, mixed>|stdClass> $rawList
     */
    private static function fromRawList(string $directory, array $rawList): DirectoryEntries
    {
        ksort($rawList, SORT_STRING);

        $entries = [];

        foreach ($rawList as $name => $value) {
            if ($name === '.' || $name === '..') {
                continue;
            }

            $path = $directory . '/' . $name;

            if (is_array($value)) {
                /** @var array<array-key, array<array-key, mixed>|stdClass> $value */
                $entries[] = DirectoryEntry::subdirectory($path, self::fromRawList($path, $value));

                continue;
            }

            $entries[] = DirectoryEntry::file(
                $path,
                type_optional(type_integer())->assert($value->size ?? null),
                isset($value->mtime) ? type_datetime()->cast($value->mtime) : null,
            );
        }

        return new DirectoryEntries($entries);
    }
}
