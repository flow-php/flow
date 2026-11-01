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

    // NativeLocalFilesystem::statFor() is `filesize($p) ?: null`, so a zero-byte file reports no size
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
