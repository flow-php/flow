<?php

declare(strict_types=1);

namespace Flow\RDSL\AccessControl;

use Flow\RDSL\AccessControl;

final readonly class Except implements AccessControl
{
    public function __construct(
        private AccessControl $acl,
        private array $except = [],
    ) {
    }

    public function isAllowed(string $name) : bool
    {
        return $this->acl->isAllowed($name) && !\in_array($name, $this->except, true);
    }
}
