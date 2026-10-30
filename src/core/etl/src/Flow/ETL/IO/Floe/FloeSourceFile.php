<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Floe;

use Flow\ETL\IO\Extractor\File\ByteUnit;
use Flow\ETL\IO\Extractor\File\SelfDescribingFile;
use Flow\ETL\IO\Extractor\File\SourceFile;
use Flow\ETL\IO\Extractor\Statistics;
use Flow\ETL\Storage\Floe\Footer;
use Flow\ETL\Values\Cardinality;
use Flow\ETL\Values\Schema;

use function array_values;
use function count;
use function max;

final readonly class FloeSourceFile implements SelfDescribingFile
{
    public function __construct(
        private Footer $footer,
        private SourceFile $source,
    ) {}

    /**
     * Holds no stream: the footer was read when it was built.
     */
    public function close(): void {}

    public function schema(): Schema
    {
        return $this->footer->schema();
    }

    public function source(): SourceFile
    {
        return $this->source;
    }

    public function statistics(): Statistics
    {
        return new Statistics(
            rows: Cardinality::exact($this->footer->statistics->rows),
            size: Cardinality::exact($this->footer->statistics->byteSize),
        );
    }

    /**
     * One unit per section: from its offset to the next section's, the last one to the end of the file.
     */
    public function units(): array
    {
        $sections = array_values($this->footer->sections);
        $count = count($sections);
        $units = [];

        foreach ($sections as $i => $section) {
            $units[] = new ByteUnit(
                $section->offset,
                max(0, (($i + 1) < $count ? $sections[$i + 1]->offset : $this->source->size ?? 0) - $section->offset),
                $section->rowCount,
            );
        }

        return $units;
    }
}
