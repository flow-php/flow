<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Mother;

use Flow\Telemetry\Context\Context;
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Context\Scope;

final class ScopeMother
{
    /**
     * A real, attached Scope backed by its own storage - detaching it cannot disturb anything else.
     */
    public static function attached(): Scope
    {
        return (new MemoryContextStorage())->attach(Context::root());
    }
}
