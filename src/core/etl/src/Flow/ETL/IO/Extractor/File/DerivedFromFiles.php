<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Extractor\File;

use Flow\ETL\Foundation\Exception\InvalidLogicException;
use Flow\ETL\IO\Extractor\Statistics;
use Flow\ETL\Values\Cardinality;
use Flow\ETL\Values\Schema;
use Generator;

final class DerivedFromFiles
{
    private ?Schema $schema = null;

    private string $from = '';

    private ?FooterStatistics $footers = null;

    /**
     * Schema::merge() returns its argument on an empty receiver, so first-file-only is this same fold
     * with a break. The first file stays open: whoever yielded it keeps it for the read that follows, or closes it.
     *
     * @param Generator<int, SelfDescribingFile> $files an unstarted generator, keyed from 0
     */
    public function fold(Generator $files, bool $unionByName): Schema
    {
        if ($this->schema !== null) {
            return $this->schema;
        }

        $schema = new Schema();
        $statistics = new Statistics(rows: Cardinality::exact(0), size: Cardinality::exact(0));
        $read = 0;
        $this->from = '';

        foreach ($files as $index => $file) {
            try {
                if ($this->from === '') {
                    $this->from = $file->source()->uri();
                }

                $schema = $schema->merge($file->schema());
                $statistics = $statistics->merge($file->statistics());
                $read++;
            } finally {
                if ($index > 0) {
                    $file->close();
                }
            }

            if (!$unionByName) {
                break;
            }
        }

        $this->footers = new FooterStatistics($statistics, $read);

        return $this->schema = $schema;
    }

    /**
     * The footers the fold reads, derived once: statistics() never opens a footer schema() already did.
     *
     * @param Generator<int, SelfDescribingFile> $files an unstarted generator
     */
    public function footers(Generator $files, bool $unionByName): FooterStatistics
    {
        $this->fold($files, $unionByName);

        return $this->footers ?? throw InvalidLogicException::because('No footer was folded: the schema was learned');
    }

    /**
     * The file the folded or learned schema was read from; '' until a fold or learn() ran, or when none was named.
     */
    public function from(): string
    {
        return $this->from;
    }

    public function schema(): ?Schema
    {
        return $this->schema;
    }

    public function learn(Schema $schema, string $from = ''): Schema
    {
        $this->from = $from;

        return $this->schema = $schema;
    }
}
