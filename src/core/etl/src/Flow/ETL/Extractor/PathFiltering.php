<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Schema;
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Filter;
use Flow\Filesystem\Path\Filter\Filters;
use Flow\Filesystem\Path\Filter\OnlyFiles;
use Generator;

trait PathFiltering
{
    /**
     * Every setter that changes what the fold would read has to clear this.
     */
    private ?Schema $derivedSchema = null;

    private string $derivedFrom = '';

    private ?Filter $filter = null;

    /**
     * Which partition columns a read discovers is a function of the path and this filter, so the
     * listing is cached next to the filter that invalidates it: one listing per extractor instance,
     * however often schema() is asked.
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

    public function filter(): Filter
    {
        return $this->filter ?? new OnlyFiles();
    }

    /**
     * @return array<string, bool>
     */
    public function partitionNames(PartitionColumns $partitionColumns, Path $path): array
    {
        return $this->partitionNames ??= $partitionColumns->names($path, $this->filter());
    }

    public function withPathFilter(Filter $filter): static
    {
        $this->partitionNames = null;
        $this->derivedSchema = null;

        if ($this->filter === null) {
            $this->filter = $filter;

            return $this;
        }

        if ($this->filter instanceof Filters) {
            $this->filter = $this->filter->add($filter);

            return $this;
        }

        $this->filter = new Filters($this->filter, $filter);

        return $this;
    }
}
