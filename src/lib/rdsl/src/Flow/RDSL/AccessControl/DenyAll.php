<?php declare(strict_types=1);

namespace Flow\RDSL\AccessControl;

use Flow\RDSL\AccessControl;

final class DenyAll implements AccessControl
{
    public function isAllowed(string $name) : bool
    {
        return false;
    }
}
