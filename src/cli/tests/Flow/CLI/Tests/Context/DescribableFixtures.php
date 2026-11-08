<?php

declare(strict_types=1);

namespace Flow\CLI\Tests\Context;

use Closure;
use Flow\ETL\Foundation\Describe\Describable;

/**
 * One instance of every Describable this package ships, with every constructor argument away from its default, so a
 * parameter that describe() or fromDescription() forgets changes the rebuilt object.
 */
final class DescribableFixtures
{
    /**
     * @return array<class-string<Describable>, Closure(): Describable>
     */
    public static function all(): array
    {
        return [];
    }
}
