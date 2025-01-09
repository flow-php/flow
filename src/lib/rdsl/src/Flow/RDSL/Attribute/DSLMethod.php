<?php

declare(strict_types=1);

namespace Flow\RDSL\Attribute;

#[\Attribute(\Attribute::TARGET_METHOD)]
final readonly class DSLMethod
{
    public function __construct(public bool $exclude = false)
    {

    }
}
