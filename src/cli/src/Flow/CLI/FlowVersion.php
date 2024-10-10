<?php

declare(strict_types=1);

namespace Flow\CLI;

/**
 * @internal
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
