<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\ErrorHandler\ThrowError;
use Flow\ETL\Filesystem\FilesystemStreams;
use Flow\ETL\Row\EntryFactory;

/**
 * Mutable Flow execution context.
 * It can be modified through the DataFrame.
 */
final class FlowContext
{
    private bool $appendSafe = false;

    private ErrorHandler $errorHandler;

    public function __construct(public readonly Config $config)
    {
        $this->errorHandler = new ThrowError();
    }

    public function appendSafe() : bool
    {
        return $this->appendSafe;
    }

    public function cache() : Cache
    {
        return $this->config->cache();
    }

    public function entryFactory() : EntryFactory
    {
        return $this->config->entryFactory();
    }

    public function errorHandler() : ErrorHandler
    {
        return $this->errorHandler;
    }

    public function setAppendSafe(bool $appendSafe = true) : self
    {
        $this->appendSafe = $appendSafe;

        return $this;
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
