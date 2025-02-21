<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk;

interface InsertOptions
{
    /**
     * @param array<mixed> $options
     */
    public static function fromArray(array $options) : self;

    public static function new() : self;
}
