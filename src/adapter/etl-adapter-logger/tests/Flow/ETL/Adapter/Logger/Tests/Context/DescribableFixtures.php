<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Logger\Tests\Context;

use Closure;
use Flow\ETL\Adapter\Logger\IO\Loader\PsrLoggerLoader;
use Flow\ETL\Foundation\Describe\Describable;
use Psr\Log\LogLevel;
use Psr\Log\NullLogger;

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
            PsrLoggerLoader::class => static fn(): Describable => new PsrLoggerLoader(
                new NullLogger(),
                'row',
                LogLevel::INFO,
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
            PsrLoggerLoader::class,
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
