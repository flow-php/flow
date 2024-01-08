<?php declare(strict_types=1);

namespace Flow\RDSL\AccessControl;

use Flow\RDSL\AccessControl;

final class DenyList implements AccessControl
{
    /**
     * @param array<string> $denyList
     */
    public function __construct(private readonly array $denyList = [])
    {
    }

    public function isAllowed(string $name) : bool
    {
        return !\in_array($name, $this->denyList, true);
    }
}
