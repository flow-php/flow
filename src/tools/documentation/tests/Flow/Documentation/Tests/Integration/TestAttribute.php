<?php

declare(strict_types=1);

namespace Flow\Documentation\Tests\Integration;

use Attribute;
use Flow\Documentation\Attribute\Module;

#[Attribute(Attribute::TARGET_FUNCTION)]
final class TestAttribute
{
    public function __construct(
        public string $name,
        public bool $active,
        public Module $module,
        public int $limit,
    ) {}
}
