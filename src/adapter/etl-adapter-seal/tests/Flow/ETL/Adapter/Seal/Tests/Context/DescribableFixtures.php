<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Seal\Tests\Context;

use Closure;
use CmsIg\Seal\Adapter\Memory\MemoryAdapter;
use CmsIg\Seal\Engine;
use CmsIg\Seal\Schema\Schema as SealSchema;
use Flow\ETL\Adapter\Seal\IO\Loader\SealLoader;
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
        return [
            SealLoader::class => static fn(): Describable => new SealLoader(
                new Engine(new MemoryAdapter(), new SealSchema([])),
                'users',
            ),
        ];
    }

    /**
     * The classes whose describe() is null for every instance: they hold a live client, a closure or the runtime.
     *
     * @return list<class-string<Describable>>
     */
    public static function undescribable(): array
    {
        return [
            SealLoader::class,
        ];
    }

    /**
     * @return list<class-string<Describable>>
     */
    public static function unconstructable(): array
    {
        return [];
    }
}
