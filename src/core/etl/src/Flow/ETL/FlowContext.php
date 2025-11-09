<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\ErrorHandler\ThrowError;
use Flow\ETL\Filesystem\FilesystemStreams;
use Flow\ETL\Function\{ExecutionMode, Functions};
use Flow\ETL\Row\EntryFactory;
use Flow\Filesystem\{Filesystem, Path, Protocol};

/**
 * Mutable Flow execution context.
 * It can be modified through the DataFrame.
 */
final class FlowContext
{
    private ErrorHandler $errorHandler;

    private readonly Functions $functions;

    public function __construct(public readonly Config $config)
    {
        $this->errorHandler = new ThrowError();
        $this->functions = new Functions(ExecutionMode::LENIENT);
    }

    public function cache() : Cache
    {
        return $this->config->cache->cache;
    }

    public function entryFactory() : EntryFactory
    {
        return $this->config->entryFactory();
    }

    public function errorHandler() : ErrorHandler
    {
        return $this->errorHandler;
    }

    public function filesystem(Path|Protocol $path) : Filesystem
    {
        return $this->config->fstab()->for($path);
    }

    public function functions() : Functions
    {
        return $this->functions;
    }

    public function setErrorHandler(ErrorHandler $handler) : self
    {
        $this->errorHandler = $handler;

        return $this;
    }

    public function streams() : FilesystemStreams
    {
        return $this->config->filesystemStreams();
    }
}
