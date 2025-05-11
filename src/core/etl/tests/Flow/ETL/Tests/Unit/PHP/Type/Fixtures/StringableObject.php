<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\PHP\Type\Fixtures;

final class StringableObject implements \Stringable
{
    public function __toString() : string
    {
        return '';
    }
}
