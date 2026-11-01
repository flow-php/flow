<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Loader\File;

use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\IO\Filesystem\SaveMode;
use Flow\ETL\IO\Filesystem\VisibleFiles;
use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\FileStatus;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Partition;
use Flow\Filesystem\Path;
use Throwable;

use function array_key_exists;
use function count;
use function hash;
use function max;
use function sort;
use function strlen;
use function substr;

final class FileCommit
{
    private const string TEMPORARY_OUTPUT_PREFIX = '.flow-tmp-';

    /**
     * Paths a serial write brought into existence in place, so abort() can remove exactly those.
     *
     * @var array<string, Path>
     */
    private array $created = [];

    /** @var array<string, DestinationStream> */
    private array $streams = [];

    /**
     * The directory the parts land in: the static part of a pattern, the parent of a plain file.
     */
    private readonly Path $root;

    private readonly Path $temporary;

    private bool $partitioned = false;

    private bool $staged = false;

    /**
     * @param Path $destination the destination as given, {part} unfilled
     * @param non-empty-string $run
     * @param int<1, max> $tasks
     * @param null|Part $task the task a shipped loader writes as; null for the job (a serial write, or the parent of
     *                        a parallel one)
     *
     * @throws RuntimeException when the destination has no extension, or is a pattern without partition placeholders
     */
    public function __construct(
        private readonly Filesystem $filesystem,
        private readonly Path $destination,
        private readonly SaveMode $saveMode,
        private readonly string $run,
        private readonly int $tasks = 1,
        private readonly ?Part $task = null,
    ) {
        if (!$destination->extension()) {
            throw new RuntimeException('Stream path must have an extension, given: ' . $destination->uri());
        }

        if (
            (new Part(0, 1, $run, false))->fill($destination)->isPattern()
            && [] === $destination->partitionPlaceholders()
        ) {
            throw new RuntimeException("Destination path can't be pattern, given: " . $destination->uri());
        }

        $this->root = $destination->isPattern() ? $destination->staticPart() : $destination->parentDirectory();
        // the destination tells apart two loaders of one job writing under the same root
        $this->temporary = $this->root->suffix(
            '/' . self::TEMPORARY_OUTPUT_PREFIX . $run . '-' . hash('xxh32', $destination->uri()),
        );
    }

    /**
     * The job commit of a write as $tasks tasks of run $run; null when the save mode does not admit the destination.
     *
     * @param bool $partitioned the files sit in partition directories under the static root
     * @param non-empty-string $run
     * @param int<1, max> $tasks
     *
     * @throws RuntimeException
     */
    public static function admitted(
        Filesystem $filesystem,
        Path $destination,
        SaveMode $saveMode,
        bool $partitioned,
        string $run,
        int $tasks,
    ): ?self {
        $commit = new self($filesystem, $destination, $saveMode, $run, $tasks);

        return $commit->admit($partitioned) ? $commit : null;
    }

    /**
     * The save mode's decision over the whole destination; a write that is not admitted writes nothing.
     *
     * @param bool $partitioned the files sit in partition directories under the static root
     *
     * @throws RuntimeException
     */
    public function admit(bool $partitioned): bool
    {
        $this->partitioned = $partitioned;

        return $this->saveMode->admits($this->filesystem, Part::glob($this->destination, $partitioned));
    }

    /**
     * Closes every stream, then removes what this write produced: a task its own staged files, the job every staged
     * file and what it created in place. The first close failure is rethrown after.
     *
     * @throws Throwable
     */
    public function abort(): void
    {
        $failure = $this->closeStreams();
        $created = $this->created;
        $this->created = [];

        if ($this->task !== null) {
            $this->removeDirectory($this->temporary->suffix('/' . $this->task->name()));
        } else {
            foreach ($created as $path) {
                if ($this->filesystem->status($path) !== null) {
                    $this->filesystem->rm($path);
                }
            }

            $this->removeDirectory($this->temporary);
        }

        if ($failure !== null) {
            throw $failure;
        }
    }

    /**
     * Every file tasks $tasks staged moves to its place under the root in task-index order, then Overwrite removes
     * the stale files the moved ones replace, and the temporary output goes. A move that fails leaves the old files
     * in place: a failed commit duplicates, it never loses. A task whose result never arrived commits nothing.
     *
     * @param list<int> $tasks the indices of the tasks whose results arrived
     *
     * @throws Throwable
     */
    public function commit(array $tasks): void
    {
        $failure = $this->closeStreams();

        if ($failure !== null) {
            throw $failure;
        }

        // a serial write in place staged nothing: there is nothing to list
        if ($this->created !== [] && !$this->staged) {
            $this->created = [];

            return;
        }

        $staged = $this->staged($tasks);

        foreach ($staged as $target => $file) {
            $this->filesystem->mv($file, Path::from($target, $this->root->options()));
        }

        if ($this->saveMode->replaces()) {
            $this->removeStale($staged);
        }

        $this->created = [];

        // the moves left the directories behind, which a listing no longer sees
        if ($staged !== []) {
            $this->filesystem->rm($this->temporary);
        } else {
            $this->removeDirectory($this->temporary);
        }
    }

    /**
     * The stream to write $partitions to: under the temporary output for a task and for a write that replaces
     * files, in place otherwise (an Append beside an existing file gets the run in its name).
     *
     * @param array<Partition> $partitions
     *
     * @throws RuntimeException
     */
    public function open(array $partitions = []): DestinationStream
    {
        $task = $this->task ?? $this->task(0);
        $destination = $this->target($task, $partitions);
        $uri = $destination->uri();

        if (array_key_exists($uri, $this->streams)) {
            return $this->streams[$uri];
        }

        if ($this->task !== null || $this->saveMode->replaces()) {
            $this->staged = true;

            return $this->streams[$uri] = $this->filesystem->writeTo($this->temporary->suffix(
                '/' . $task->name() . '/' . substr($destination->uri(), strlen($this->root->uri()) + 1),
            ));
        }

        $output =
            $this->saveMode === SaveMode::Append && $this->filesystem->status($destination) !== null
                ? $task->beside($destination)
                : $destination;
        $this->created[$uri] = $output;

        return $this->streams[$uri] = $this->filesystem->writeTo($output);
    }

    public function task(int $index): Part
    {
        return new Part(max(0, $index), $this->tasks, $this->run, $this->saveMode === SaveMode::Append);
    }

    /**
     * @return null|Throwable the first close failure, every stream closed
     */
    private function closeStreams(): ?Throwable
    {
        $streams = $this->streams;
        $this->streams = [];
        $failure = null;

        // every handle closes first: a format writer flushes its footer on close, and a file is moved or removed
        // after that, not before
        foreach ($streams as $stream) {
            if ($stream->isOpen()) {
                try {
                    $stream->close();
                } catch (Throwable $closeFailure) {
                    $failure ??= $closeFailure;
                }
            }
        }

        return $failure;
    }

    /**
     * @return list<FileStatus> the files at $glob, hidden paths below its static part excluded
     */
    private function parts(Path $glob): array
    {
        if (!$glob->isPattern()) {
            $status = $this->filesystem->status($glob);

            return $status?->isFile() === true ? [$status] : [];
        }

        $files = [];

        foreach ($this->filesystem->list($glob, new VisibleFiles($glob->staticPart())) as $status) {
            $files[] = $status;
        }

        return $files;
    }

    /**
     * Listed, not status()-checked: a memory filesystem has no status for a directory.
     */
    private function removeDirectory(Path $directory): void
    {
        foreach ($this->filesystem->list($directory->suffix('/**/*')) as $_) {
            $this->filesystem->rm($directory);

            return;
        }
    }

    /**
     * A partitioned destination keeps the partitions no row was written to.
     *
     * @param array<string, Path> $moved by target URI
     */
    private function removeStale(array $moved): void
    {
        $written = [];
        $kept = [];

        foreach ($moved as $target => $_) {
            $written[] = Path::from($target, $this->root->options());
            $kept[Path::from($target, $this->root->options())->path()] = true;
        }

        // {part} counts among the placeholders until it is filled
        $filled = $this->task(0)->fill($this->destination);

        if ($moved === [] && !$this->partitioned && [] === $filled->partitionPlaceholders()) {
            $written[] = $filled;
        }

        $globs = [];

        foreach ($written as $file) {
            foreach (Part::stale($file, $this->destination) as $glob) {
                $globs[$glob->uri()] = $glob;
            }
        }

        foreach ($globs as $glob) {
            foreach ($this->parts($glob) as $stale) {
                // by path: a filesystem may list under another scheme than it was given
                if (!array_key_exists($stale->path->path(), $kept)) {
                    $this->filesystem->rm($stale->path);
                }
            }
        }
    }

    /**
     * Every file tasks $tasks staged, in task-index order, by the URI it commits to.
     *
     * @param list<int> $tasks
     *
     * @return array<string, Path>
     */
    private function staged(array $tasks): array
    {
        sort($tasks);
        $files = [];

        foreach ($tasks as $index) {
            $directory = $this->temporary->suffix('/' . $this->task($index)->name());

            foreach ($this->filesystem->list($directory->suffix('/**/*')) as $status) {
                if ($status->isFile()) {
                    // rebuilt from the directory: a filesystem may list under another scheme than it was given
                    $relative = substr($status->path->path(), strlen($directory->path()));
                    $files[$this->root->suffix($relative)->uri()] = $directory->suffix($relative);
                }
            }
        }

        return $files;
    }

    /**
     * @param array<Partition> $partitions
     *
     * @throws RuntimeException
     */
    private function target(Part $task, array $partitions): Path
    {
        $destination = $task->fill($this->destination);

        if (count($partitions)) {
            return $destination->addPartitions(...$partitions);
        }

        if ([] !== $destination->partitionPlaceholders()) {
            throw new RuntimeException(
                'Destination path "'
                . $destination->uri()
                . '" contains partition placeholders but rows are not partitioned, add partitionBy() to your pipeline',
            );
        }

        return $destination;
    }
}
