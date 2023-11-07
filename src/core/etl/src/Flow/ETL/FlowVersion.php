<?php

declare(strict_types=1);

namespace Flow\ETL;

/**
 * @interal
 */
final class FlowVersion
{
    /**
     * Content of this method is replaced by Box library during PHAR generation.
     */
    public static function getVersion() : string
    {
        return '@git_version@';
    }
}
