<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Cardinality;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\Filesystem\FileStatus;

final readonly class ListedFiles
{
    /**
     * @var int<0, max>
     */
    public int $count;

    /**
     * @param Cardinality $bytes total listed bytes, unknown when ANY member did not report a size
     */
    public function __construct(
        int $count,
        public Cardinality $bytes,
    ) {
        if ($count < 0) {
            throw new InvalidArgumentException('Listed file count must not be negative, given: ' . $count);
        }

        $this->count = $count;
    }

    /**
     * One pass, nothing kept but the two totals. NativeLocalFilesystem::statFor() is `filesize($p) ?: null`, so it
     * reports a zero-byte file as null.
     *
     * @param iterable<FileStatus|SourceFile> $files
     */
    public static function of(iterable $files): self
    {
        $count = 0;
        $total = 0;
        $sized = true;

        foreach ($files as $file) {
            $count++;

            if ($file->size === null) {
                $sized = false;

                continue;
            }

            $total += $file->size;
        }

        return new self($count, $sized ? Cardinality::exact($total) : Cardinality::unknown());
    }
}
