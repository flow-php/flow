<?php declare(strict_types=1);

namespace Flow\RDSL\AccessControl;

use Flow\RDSL\AccessControl;

final class AllowAll implements AccessControl
{
    public function isAllowed(string $name) : bool
    {
        return true;
    }
}
