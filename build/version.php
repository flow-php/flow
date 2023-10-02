<?php

declare(strict_types=1);

final class FlowVersion
{
    public static function getVersion(): string
    {
        return '1.0.x-@git_commit_short@';
    }
}
