<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use DateTimeImmutable;
use Flow\ETL\Exception\RequiredPHPVersionException;
use Flow\ETL\Function;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Tests\Context\ScalarFunctionClasses;
use Flow\ETL\Tests\Context\ScalarFunctionFixtures;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\now;
use function Flow\ETL\DSL\random_string;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\ulid;
use function Flow\ETL\DSL\uuid_v4;
use function Flow\ETL\DSL\uuid_v7;
use function in_array;

final class AllFunctionsDeclareTheirDeterminismTest extends FlowTestCase
{
    public static function scalar_functions_provider(): Generator
    {
        foreach (ScalarFunctionClasses::all() as $class) {
            yield $class => [$class];
        }
    }

    /**
     * @param class-string<ScalarFunction> $class
     */
    #[DataProvider('scalar_functions_provider')]
    public function test_every_scalar_function_is_deterministic_except_the_declared_generators(string $class): void
    {
        try {
            $function = ScalarFunctionFixtures::instance($class);
        } catch (RequiredPHPVersionException $e) {
            static::markTestSkipped($e->getMessage());
        }

        // the fixtures build Uuid through uuid4() and Ulid without a ref - both generate
        static::assertSame(
            !in_array(
                $class,
                [Function\Now::class, Function\RandomString::class, Function\Uuid::class, Function\Ulid::class],
                true,
            ),
            $function->deterministic(),
        );
    }

    public function test_a_composite_is_not_deterministic_when_any_child_is_not(): void
    {
        static::assertFalse(ref('a')->equals(now())->deterministic());
        static::assertFalse(now()->and(lit(true))->deterministic());
        static::assertFalse(lit(1)->plus(random_string(3))->deterministic());
        static::assertTrue(ref('a')->equals(lit(1))->and(ref('b')->isNotNull())->deterministic());
    }

    public function test_a_ulid_over_a_reference_is_deterministic(): void
    {
        static::assertTrue(ulid(ref('id'))->deterministic());
        static::assertFalse(ulid()->deterministic());
    }

    public function test_both_uuid_constructors_generate(): void
    {
        static::assertFalse(uuid_v4()->deterministic());
        static::assertFalse(uuid_v7(new DateTimeImmutable('2024-01-01 00:00:00'))->deterministic());
    }
}
