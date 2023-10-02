<?php

declare(strict_types=1);

final class FlowVersion
{
    public static function getVersion() : string
    {
        return '@git_version@';
    }
}
