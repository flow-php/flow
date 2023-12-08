<?php declare(strict_types=1);

namespace Flow\RDSL\AccessControl;

use Flow\RDSL\AccessControl;

final class Except implements AccessControl
{
    public function __construct(
        private readonly AccessControl $acl,
        private readonly array $except = []
    ) {
    }

    public function isAllowed(string $name) : bool
    {
        return $this->acl->isAllowed($name) && !\in_array($name, $this->except, true);
    }
}
