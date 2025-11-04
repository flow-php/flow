<?php

declare(strict_types=1);

namespace Flow\Website\Service\Example;

final readonly class Output
{
    public function __construct(public string $content, public string $type)
    {
    }
}
