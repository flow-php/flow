<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Extractor\File;

use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Filesystem\VisibleFiles;
use Flow\ETL\Values\Cardinality;
use Flow\Filesystem\FileListing;
use Flow\Filesystem\FileStatus;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Filter;
use Flow\Filesystem\Path\Filter\OnlyFiles;

use function array_key_exists;
use function array_merge;
use function count;
use function iterator_to_array;
use function ksort;

final class SourceFiles
{
    /**
     * @var null|list<FileStatus>
     */
    private ?array $statuses = null;

    /**
     * @var null|array<string, bool>
     */
    private ?array $partitionNames = null;

    public function __construct(
        public readonly Filesystem $filesystem,
        public readonly Path $path,
    ) {}

    /**
     * The split's path, listed, when $criteria has a split; otherwise the source's files, listed once until refresh(),
     * that $criteria's path filter admits.
     *
     * @return list<FileStatus>
     */
    public function statuses(Criteria $criteria = new Criteria()): array
    {
        if ($criteria->split !== null) {
            return iterator_to_array(
                (new FileListing($this->filesystem))->list(
                    $criteria->split->path,
                    new VisibleFiles($criteria->split->path->staticPart()),
                ),
                false,
            );
        }

        $this->statuses ??= iterator_to_array(
            (new FileListing($this->filesystem))->list($this->path, new VisibleFiles($this->path->staticPart())),
            false,
        );

        if ($criteria->pathFilter instanceof OnlyFiles) {
            return $this->statuses;
        }

        $admitted = [];

        foreach ($this->statuses as $status) {
            if ($criteria->pathFilter->accept($status)) {
                $admitted[] = $status;
            }
        }

        return $admitted;
    }

    /**
     * The files a scan reads: a sized split names its file without listing; an unsized split lists its own path (a
     * split over the source itself, an empty listing cut into one split, still admits nothing); no split reads what
     * the path filter admits.
     *
     * @return list<SourceFile>
     */
    public function of(Criteria $criteria): array
    {
        if ($criteria->split !== null && $criteria->split->size !== null) {
            return [new SourceFile($criteria->split->path, $criteria->split->size)];
        }

        $sources = [];

        foreach ($this->statuses($criteria) as $status) {
            $sources[] = new SourceFile($status->path, $status->size);
        }

        return $sources;
    }

    /**
     * Partition name => nullable. A partition name some listed path lacks, or holds no value for, is nullable.
     *
     * @return array<string, bool>
     */
    public function partitionNames(): array
    {
        if ($this->partitionNames !== null) {
            return $this->partitionNames;
        }

        $counts = [];
        $nullable = [];
        $statuses = $this->statuses();

        foreach ($statuses as $status) {
            foreach ($status->path->partitions() as $partition) {
                $counts[$partition->name] = ($counts[$partition->name] ?? 0) + 1;

                if ($partition->value === null) {
                    $nullable[$partition->name] = true;
                }
            }
        }

        $names = [];

        foreach ($counts as $name => $count) {
            $names[$name] = $count < count($statuses) || array_key_exists($name, $nullable);
        }

        ksort($names);

        return $this->partitionNames = $names;
    }

    /**
     * @return int<0, max>
     */
    public function count(): int
    {
        return count($this->statuses());
    }

    /**
     * Unknown when any listed file reports no size. NativeLocalFilesystem::statFor() is `filesize($p) ?: null`, so it
     * reports a zero-byte file as null.
     */
    public function bytes(): Cardinality
    {
        $total = 0;

        foreach ($this->statuses() as $status) {
            if ($status->size === null) {
                return Cardinality::unknown();
            }

            $total += $status->size;
        }

        return Cardinality::exact($total);
    }

    /**
     * One split per admitted file, in listing order, each covering the whole file. An empty listing gets one split
     * over the path itself, which reads nothing.
     *
     * @return non-empty-list<FileSplit>
     */
    public function perFile(Filter $pathFilter = new OnlyFiles()): array
    {
        $splits = [];

        foreach ($this->statuses(new Criteria(pathFilter: $pathFilter)) as $status) {
            $splits[] = new FileSplit($status->path, 0, null, $status->size);
        }

        return $splits === [] ? [new FileSplit($this->path, 0, null)] : $splits;
    }

    /**
     * FileSplit::windows() over every admitted file, in listing order. An empty listing is one split over the path
     * itself, which reads nothing.
     *
     * @param int<1, max> $targetSplitBytes
     *
     * @return non-empty-list<FileSplit>
     */
    public function byteWindows(int $targetSplitBytes, Filter $pathFilter = new OnlyFiles()): array
    {
        $splits = [];

        foreach ($this->statuses(new Criteria(pathFilter: $pathFilter)) as $status) {
            $splits[] = FileSplit::windows(new SourceFile($status->path, $status->size), $targetSplitBytes);
        }

        return $splits === [] ? [new FileSplit($this->path, 0, null)] : array_merge(...$splits);
    }

    public function refresh(): void
    {
        $this->statuses = null;
        $this->partitionNames = null;
    }
}
