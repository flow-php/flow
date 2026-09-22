<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Cardinality;
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
     * The footers the fold read while deriving the schema; set together with $derivedSchema.
     */
    private ?FooterStatistics $derivedFooters = null;

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
        $statistics = new Statistics(rows: Cardinality::exact(0), size: Cardinality::exact(0));
        $read = 0;
        $this->derivedFrom = '';

        foreach ($files as $file) {
            try {
                if ($this->derivedFrom === '') {
                    $this->derivedFrom = $file->source()->uri();
                }

                $schema = $schema->merge($file->schema());
                $statistics = $statistics->merge($file->statistics());
                $read++;
            } finally {
                $file->close();
            }

            if (!$unionByName) {
                break;
            }
        }

        $this->derivedFooters = new FooterStatistics($statistics, $read);

        return $this->derivedSchema = $schema;
    }

    /**
     * The footers the schema fold reads, derived once: statistics() never opens a footer schema() already did.
     *
     * @param Generator<int, SelfDescribingFile> $files an unstarted generator
     */
    private function derivedFooters(Generator $files, bool $unionByName): FooterStatistics
    {
        $this->derivedSchema($files, $unionByName);

        return $this->derivedFooters ?? new FooterStatistics(new Statistics(), 0);
    }

    /**
     * The same footers without deriving a schema, for a read whose schema was declared: nothing else opens them.
     *
     * @param Generator<int, SelfDescribingFile> $files an unstarted generator
     */
    private function footers(Generator $files, bool $unionByName): FooterStatistics
    {
        $statistics = new Statistics(rows: Cardinality::exact(0), size: Cardinality::exact(0));
        $read = 0;

        foreach ($files as $file) {
            try {
                $statistics = $statistics->merge($file->statistics());
                $read++;
            } finally {
                $file->close();
            }

            if (!$unionByName) {
                break;
            }
        }

        return new FooterStatistics($statistics, $read);
    }

    /**
     * @return array<string, bool>
     */
    public function partitionNames(PartitionColumns $partitionColumns, Path $path): array
    {
        return $this->partitionNames ??= $partitionColumns->names($path, new OnlyFiles());
    }
}
