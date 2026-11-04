<?php

declare(strict_types=1);

namespace Flow\Documentation\Tests\Double\Api;

trait Chain
{
    public function chained(): static
    {
        return $this;
    }
}
