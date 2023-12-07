<?php declare(strict_types=1);

namespace Flow\RDSL\AccessControl;

use Flow\RDSL\AccessControl;

final class AllowList implements AccessControl
{
    /**
     * @param array<string> $allowList
     */
    public function __construct(private readonly array $allowList = [])
    {
    }

    public function isAllowed(string $name) : bool
    {
        return \in_array($name, $this->allowList, true);
    }
}
