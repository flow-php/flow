<?php

declare(strict_types=1);

namespace Flow\Website\Service\Example;

final class Output
{
    public function __construct(public readonly string $content, public readonly string $type)
    {
    }
}
