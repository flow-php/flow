<?php

declare(strict_types=1);

namespace Flow\CLI\Tests\Context;

use Closure;
use Flow\CLI\Loader\ProgressBarLoader;
use Flow\ETL\Foundation\Describe\Describable;
use Symfony\Component\Console\Helper\ProgressBar;
use Symfony\Component\Console\Output\NullOutput;

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
            ProgressBarLoader::class => static fn(): Describable => new ProgressBarLoader(new ProgressBar(
                new NullOutput(),
            )),
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
            ProgressBarLoader::class,
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
