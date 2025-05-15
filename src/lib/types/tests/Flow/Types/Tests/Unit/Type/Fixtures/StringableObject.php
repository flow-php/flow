<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Fixtures;

final class StringableObject implements \Stringable
{
    public function __toString() : string
    {
        return '';
    }
}
