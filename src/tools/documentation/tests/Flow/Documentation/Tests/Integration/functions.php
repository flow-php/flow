<?php

declare(strict_types=1);

namespace Flow\Documentation\Tests\Integration;

use Flow\Documentation\Tests\Integration\Double\ParameterClass;

#[TestAttribute(name: 'test', active: true)]
function doSomething(string|int|float|\DateTimeInterface|ParameterClass $argument) : ?bool
{
}
