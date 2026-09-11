<?php

declare(strict_types=1);

namespace Flow\Documentation\Tests\Double;

class FluentParent
{
    public function fluent(): static
    {
        return $this;
    }
}
