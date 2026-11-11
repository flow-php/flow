<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use ArrayObject;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Progress;
use Flow\ETL\IO\Filesystem\SaveMode;
use Flow\ETL\IO\Loader\Closure;
use Flow\ETL\IO\Loader\Discardable;
use Flow\ETL\IO\Loader\File\FileCommit;
use Flow\ETL\IO\Loader\File\FileLoader;
use Flow\ETL\IO\Loader\File\Part;
use Flow\ETL\IO\Loader\Partitioning;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\Filesystem\Path;
use SensitiveParameter;

final readonly class CountingFileLoader implements Closure, Discardable, FileLoader
{
    /**
     * @param ArrayObject<int, true> $destinationCalls one entry per destination() call
     */
    public function __construct(
        private FileLoader&Closure&Discardable $loader,
        public ArrayObject $destinationCalls = new ArrayObject(),
    ) {}

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        throw new InvalidArgumentException('A counting loader stays in its process');
    }

    public function closure(Progress $progress): void
    {
        $this->loader->closure($progress);
    }

    public function commit(string $run, int $tasks): ?FileCommit
    {
        return $this->loader->commit($run, $tasks);
    }

    public function describe(): ?Description
    {
        return null;
    }

    public function destination(): Path
    {
        $this->destinationCalls->append(true);

        return $this->loader->destination();
    }

    public function discard(): void
    {
        $this->loader->discard();
    }

    public function inTask(Part $task): static
    {
        return new self($this->loader->inTask($task), $this->destinationCalls);
    }

    public function load(Rows $rows, Backend $backend): void
    {
        $this->loader->load($rows, $backend);
    }

    public function partitionBy(Partitioning $partitioning): static
    {
        return new self($this->loader->partitionBy($partitioning), $this->destinationCalls);
    }

    public function partitioning(): Partitioning
    {
        return $this->loader->partitioning();
    }

    public function saveMode(SaveMode $mode): static
    {
        return new self($this->loader->saveMode($mode), $this->destinationCalls);
    }
}
