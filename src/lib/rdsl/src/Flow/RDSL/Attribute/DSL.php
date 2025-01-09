<?php

declare(strict_types=1);

namespace Flow\RDSL\Attribute;

#[\Attribute(\Attribute::TARGET_FUNCTION)]
final readonly class DSL
{
    public function __construct(public bool $exclude = false)
    {

    }
}
