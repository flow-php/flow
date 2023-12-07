<?php declare(strict_types=1);

namespace Flow\RDSL;

interface AccessControl
{
    public function isAllowed(string $name) : bool;
}
