<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Exception;

use InvalidArgumentException;

final class InvalidExplainConfigException extends InvalidArgumentException
{
    public static function buffersRequiresAnalyze(): self
    {
        return new self('BUFFERS option requires ANALYZE to be enabled');
    }

    public static function timingRequiresAnalyze(): self
    {
        return new self('TIMING option requires ANALYZE to be enabled');
    }

    public static function walRequiresAnalyze(): self
    {
        return new self('WAL option requires ANALYZE to be enabled');
    }
}
