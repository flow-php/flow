<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Schema;
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Filter\OnlyFiles;
use Generator;

trait PathFiltering
{
    /**
     * Every setter that changes what the fold would read has to clear this.
     */
    private ?Schema $derivedSchema = null;

    private string $derivedFrom = '';

    /**
     * One listing per extractor instance, however often schema() is asked.
     *
     * @var null|array<string, bool>
     */
    private ?array $partitionNames = null;

    /**
     * Schema::merge() returns its argument on an empty receiver, so first-file-only is this same fold
     * with a break.
     *
     * @param Generator<int, SelfDescribingFile> $files an unstarted generator
     */
    private function derivedSchema(Generator $files, bool $unionByName): Schema
    {
        if ($this->derivedSchema !== null) {
            return $this->derivedSchema;
        }

        $schema = new Schema();
        $this->derivedFrom = '';

        foreach ($files as $file) {
            try {
                if ($this->derivedFrom === '') {
                    $this->derivedFrom = $file->source()->uri();
                }

                $schema = $schema->merge($file->schema());
            } finally {
                $file->close();
            }

            if (!$unionByName) {
                break;
            }
        }

        return $this->derivedSchema = $schema;
    }

    /**
     * @return array<string, bool>
     */
    public function partitionNames(PartitionColumns $partitionColumns, Path $path): array
    {
        return $this->partitionNames ??= $partitionColumns->names($path, new OnlyFiles());
    }
}
