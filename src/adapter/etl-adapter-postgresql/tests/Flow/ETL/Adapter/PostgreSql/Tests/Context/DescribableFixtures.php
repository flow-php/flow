<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Context;

use Closure;
use Flow\ETL\Adapter\PostgreSql\IO\Loader\PostgreSqlLoader;
use Flow\ETL\Foundation\Describe\Describable;
use Flow\PostgreSql\Tests\Mother\FakeClient;

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
            PostgreSqlLoader::class => static fn(): Describable => new PostgreSqlLoader(new FakeClient(), 'orders'),
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
            PostgreSqlLoader::class,
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
