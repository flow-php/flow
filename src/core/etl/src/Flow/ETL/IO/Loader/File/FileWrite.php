<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Loader\File;

use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\IO\Filesystem\SaveMode;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Path;
use Throwable;

final class FileWrite
{
    private readonly FileCommit $commit;

    private readonly bool $admitted;

    /**
     * @var array<string, FileWriter> by stream URI
     */
    private array $open = [];

    /**
     * A serial write ($task null) is the whole job: it admits the write now and commits it at closure(). A shipped
     * task only stages its files; the job that shipped it commits them.
     *
     * @throws RuntimeException when the save mode refuses the destination
     */
    public function __construct(
        Filesystem $filesystem,
        Path $path,
        SaveMode $saveMode,
        private readonly PartitionRouter $router,
        private readonly FileWriterFactory $writers,
        private readonly ?Part $task = null,
    ) {
        $this->commit = new FileCommit(
            $filesystem,
            $path,
            $saveMode,
            $task->run ?? Part::serialRun(),
            $task->splits ?? 1,
            $task,
        );
        $this->admitted = $task !== null || $this->commit->admit($router->partitioned());
    }

    /**
     * Closes every writer - the first close failure rethrown once all were closed - then commits a serial write.
     *
     * @throws Throwable
     */
    public function closure(): void
    {
        $failure = $this->closeWriters();

        if ($failure !== null) {
            throw $failure;
        }

        if ($this->task === null) {
            $this->commit->commit([0]);
        }
    }

    /**
     * Closes every writer, then aborts the write whatever a close threw.
     *
     * @throws Throwable
     */
    public function discard(): void
    {
        $failure = $this->closeWriters();
        $this->commit->abort();

        if ($failure !== null) {
            throw $failure;
        }
    }

    public function write(Rows $rows, Backend $backend): void
    {
        if (!$rows->count() || !$this->admitted) {
            return;
        }

        foreach ($this->router->route($rows) as [$partitions, $group]) {
            $stream = $this->commit->open($partitions->toArray());

            ($this->open[$stream->path()->uri()] ??= $this->writers->writer(
                $stream,
                $group->schema(),
                $backend,
            ))->write($group);
        }
    }

    /**
     * Closes every open writer, all of them even when one throws.
     *
     * @return null|Throwable the first close failure
     */
    private function closeWriters(): ?Throwable
    {
        $failure = null;

        foreach ($this->open as $uri => $writer) {
            unset($this->open[$uri]);

            try {
                $writer->close();
            } catch (Throwable $e) {
                $failure ??= $e;
            }
        }

        return $failure;
    }
}
