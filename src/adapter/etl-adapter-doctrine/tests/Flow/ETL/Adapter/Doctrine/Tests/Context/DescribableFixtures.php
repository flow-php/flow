<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Context;

use Closure;
use Flow\ETL\Adapter\Doctrine\IO\Loader\DbalLoader;
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
            DbalLoader::class => static fn(): Describable => new DbalLoader('orders', [
                'url' => 'pdo-sqlite:///:memory:',
            ]),
        ];
    }

    /**
     * @return list<class-string<Describable>>
     */
    public static function undescribable(): array
    {
        return [];
    }

    /**
     * @return list<class-string<Describable>>
     */
    public static function unconstructable(): array
    {
        return [];
    }
}
