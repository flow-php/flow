<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\Planning\Logical\Node;
use Flow\ETL\Planning\Logical\Node\Read;
use Flow\ETL\Planning\Logical\Trigger;
use Flow\ETL\Planning\Optimizer\Rule\PushProjectionIntoSource;

use function Flow\ETL\DSL\config;

final class ProjectionContext
{
    public static function pushed(Node $root, Trigger $trigger = Trigger::rows): Read
    {
        return (new PushProjectionIntoSource())
            ->apply($trigger->plan($root), config()->backend())
            ->source();
    }
}
