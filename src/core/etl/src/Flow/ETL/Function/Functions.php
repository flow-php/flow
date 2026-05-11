<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\Exception;

final class Functions
{
    public function __construct(
        private ExecutionMode $mode,
    ) {}

    /**
     * @throws Exception
     */
    public function invalidResult(Exception $exception): null
    {
        if ($this->mode === ExecutionMode::STRICT) {
            throw $exception;
        }

        return null;
    }

    public function setMode(ExecutionMode $executionMode): void
    {
        $this->mode = $executionMode;
    }
}
