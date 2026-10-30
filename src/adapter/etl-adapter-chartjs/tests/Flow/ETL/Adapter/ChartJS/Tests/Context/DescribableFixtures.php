<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\ChartJS\Tests\Context;

use Closure;
use Flow\ETL\Adapter\ChartJS\IO\Loader\ChartJSLoader;
use Flow\ETL\Foundation\Describe\Describable;

use function Flow\ETL\Adapter\ChartJS\bar_chart;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;

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
            ChartJSLoader::class => static fn(): Describable => new ChartJSLoader(bar_chart(
                ref('label'),
                refs(ref('value')),
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
            ChartJSLoader::class,
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
