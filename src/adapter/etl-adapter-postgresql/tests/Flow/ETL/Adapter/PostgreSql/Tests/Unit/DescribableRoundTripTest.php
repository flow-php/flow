<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Unit;

use Flow\ETL\Adapter\PostgreSql\Tests\Context\DescribableFixtures;
use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Exception\RequiredPHPVersionException;
use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\Context\ExtractorClasses;
use Flow\ETL\Tests\Context\ScalarFunctionClasses;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function array_diff;
use function array_keys;
use function in_array;

final class DescribableRoundTripTest extends FlowTestCase
{
    /**
     * @return Generator<class-string<Describable>, array{class-string<Describable>}>
     */
    public static function describables(): Generator
    {
        foreach (ScalarFunctionClasses::implementing(
            Describable::class,
            ExtractorClasses::root('adapter/etl-adapter-postgresql'),
        ) as $class) {
            yield $class => [$class];
        }
    }

    public function test_every_describable_has_a_fixture(): void
    {
        static::assertSame(
            [],
            array_diff(
                ScalarFunctionClasses::implementing(
                    Describable::class,
                    ExtractorClasses::root('adapter/etl-adapter-postgresql'),
                ),
                array_keys(DescribableFixtures::all()),
            ),
        );
    }

    /**
     * @param class-string<Describable> $class
     */
    #[DataProvider('describables')]
    public function test_a_rebuilt_describable_equals_the_described_one(string $class): void
    {
        if (in_array($class, DescribableFixtures::unconstructable(), true)) {
            $this->expectException(RuntimeException::class);
        }

        try {
            $describable = DescribableFixtures::all()[$class]();
        } catch (RequiredPHPVersionException $e) {
            static::markTestSkipped($e->getMessage());
        }

        if (in_array($class, DescribableFixtures::undescribable(), true)) {
            static::assertNull($describable->describe());

            return;
        }

        static::assertEquals($describable, DescribeRoundTrip::rebuilt($describable));
    }
}
