<?php

declare(strict_types=1);

namespace Flow\Documentation\Tests\Integration;

use DateTimeInterface;
use Flow\Documentation\Attribute\Module;
use Flow\Documentation\Tests\Integration\Double\ParameterClass;

#[TestAttribute(name: 'test', active: true, module: Module::CORE, limit: PHP_INT_MAX)]
function doSomething(string|int|float|DateTimeInterface|ParameterClass $argument): ?bool
{
    return false;
}

function doNothing($argument) {}
