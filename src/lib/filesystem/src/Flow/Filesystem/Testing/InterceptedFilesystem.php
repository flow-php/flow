<?php

declare(strict_types=1);

namespace Flow\Filesystem\Testing;

use Closure;
use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\Exception\InvalidArgumentException;
use Flow\Filesystem\FileStatus;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Mount;
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Filter;
use Flow\Filesystem\Path\Filter\KeepAll;
use Flow\Filesystem\SourceStream;
use Generator;
use SensitiveParameter;

final class InterceptedFilesystem implements Filesystem
{
    /** @var Closure(FilesystemOperation, Path): void */
    private Closure $hook;

    public function __construct(
        private readonly Filesystem $filesystem,
    ) {
        $this->hook = static function (FilesystemOperation $operation, Path $path): void {};
    }

    public static function fromArray(#[SensitiveParameter] array $configuration): static
    {
        throw new InvalidArgumentException(
            'InterceptedFilesystem holds a test hook and cannot be rebuilt from a configuration',
        );
    }

    public function appendTo(Path $path): DestinationStream
    {
        ($this->hook)(FilesystemOperation::AppendTo, $path);

        return $this->filesystem->appendTo($path);
    }

    public function create(Path $path): DestinationStream
    {
        ($this->hook)(FilesystemOperation::Create, $path);

        return $this->filesystem->create($path);
    }

    public function getSystemTmpDir(): Path
    {
        return $this->filesystem->getSystemTmpDir();
    }

    public function list(Path $path, Filter $pathFilter = new KeepAll()): Generator
    {
        ($this->hook)(FilesystemOperation::List, $path);

        // returned, not yielded from: the hook runs at the call, not at the first iteration
        return $this->filesystem->list($path, $pathFilter);
    }

    public function mount(): Mount
    {
        return $this->filesystem->mount();
    }

    public function mv(Path $from, Path $to): bool
    {
        ($this->hook)(FilesystemOperation::Mv, $from);

        return $this->filesystem->mv($from, $to);
    }

    public function normalize(): ?array
    {
        return null;
    }

    /**
     * @param callable(FilesystemOperation, Path): void $hook
     */
    public function onCall(callable $hook): self
    {
        $this->hook = $hook(...);

        return $this;
    }

    public function readFrom(Path $path): SourceStream
    {
        ($this->hook)(FilesystemOperation::ReadFrom, $path);

        return $this->filesystem->readFrom($path);
    }

    public function rm(Path $path, bool $recursive = true): bool
    {
        ($this->hook)(FilesystemOperation::Rm, $path);

        return $this->filesystem->rm($path, $recursive);
    }

    public function status(Path $path): ?FileStatus
    {
        ($this->hook)(FilesystemOperation::Status, $path);

        return $this->filesystem->status($path);
    }

    public function supports(Path $path): bool
    {
        return $this->filesystem->supports($path);
    }

    public function writeTo(Path $path): DestinationStream
    {
        ($this->hook)(FilesystemOperation::WriteTo, $path);

        return $this->filesystem->writeTo($path);
    }
}
