<?php

declare(strict_types=1);

namespace Flow\Documentation\Tests\Integration;

#[\Attribute(\Attribute::TARGET_FUNCTION)]
final class TestAttribute
{
    public function __construct(
        public string $name,
        public bool $active,
    ) {

    }
}
