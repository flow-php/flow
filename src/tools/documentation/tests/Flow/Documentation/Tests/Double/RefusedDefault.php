<?php

declare(strict_types=1);

namespace Flow\Documentation\Tests\Double;

use RuntimeException;

final class RefusedDefault
{
    public function __construct()
    {
        throw new RuntimeException('The extension refused to load');
    }
}
