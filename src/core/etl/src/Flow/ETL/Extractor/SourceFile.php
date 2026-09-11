<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\Filesystem\Path;

/**
 * The partition values come from the path, so a read knows them without opening the file.
 */
final readonly class SourceFile
{
    /**
     * @var array<string, null|string>
     */
    public array $partitionValues;

    public function __construct(
        public Path $path,
    ) {
        $values = [];

        foreach ($path->partitions() as $partition) {
            $values[$partition->name] = $partition->value;
        }

        $this->partitionValues = $values;
    }

    public function uri(): string
    {
        return $this->path->uri();
    }
}
