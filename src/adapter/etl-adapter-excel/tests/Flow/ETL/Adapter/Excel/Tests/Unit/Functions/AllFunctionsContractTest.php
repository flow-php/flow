<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Unit\Functions;

use Flow\ETL\Adapter\Excel\Functions\IsValidExcelSheetName;
use Flow\ETL\Adapter\Excel\Tests\Context\DescribableFixtures;
use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\Tests\Context\ExtractorClasses;
use Flow\ETL\Tests\Context\ScalarFunctionClasses;
use Flow\ETL\Tests\Context\ScalarFunctionContract;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Type;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_is_nullable;
use function Flow\Types\DSL\type_optional;

/**
 * etl's function contracts, run over every ScalarFunction this package ships: a new function joins them without a
 * new test.
 */
final class AllFunctionsContractTest extends FlowTestCase
{
    /**
     * @return Generator<class-string<ScalarFunction>, array{class-string<ScalarFunction>}>
     */
    public static function functions(): Generator
    {
        foreach (ScalarFunctionClasses::all(ExtractorClasses::root('adapter/etl-adapter-excel')) as $class) {
            yield $class => [$class];
        }
    }

    /**
     * @return Generator<class-string<ScalarFunction>, array{class-string<ScalarFunction>}>
     */
    public static function declared_nullable(): Generator
    {
        yield IsValidExcelSheetName::class => [IsValidExcelSheetName::class];
    }

    /**
     * @return array<class-string<ScalarFunction>, Type<mixed>>
     */
    public static function expected_returns(): array
    {
        return [IsValidExcelSheetName::class => type_optional(type_boolean())];
    }

    /**
     * @param class-string<ScalarFunction> $class
     */
    public static function instance(string $class): ScalarFunction
    {
        return type_instance_of(ScalarFunction::class)->assert(DescribableFixtures::all()[$class]());
    }

    /**
     * @param class-string<ScalarFunction> $class
     */
    #[DataProvider('functions')]
    public function test_every_function_returns_its_declared_type(string $class): void
    {
        ScalarFunctionContract::returnsItsDeclaredType(self::instance($class), self::expected_returns()[$class]);
    }

    /**
     * @param class-string<ScalarFunction> $class
     */
    #[DataProvider('declared_nullable')]
    public function test_a_declared_nullable_function_returns_a_nullable_type(string $class): void
    {
        static::assertTrue(type_is_nullable(self::expected_returns()[$class]));
        ScalarFunctionContract::returnsItsDeclaredType(self::instance($class), self::expected_returns()[$class]);
    }

    /**
     * @param class-string<ScalarFunction> $class
     */
    #[DataProvider('functions')]
    public function test_with_children_round_trips(string $class): void
    {
        ScalarFunctionContract::withChildrenRoundTrips(self::instance($class));
    }

    /**
     * @param class-string<ScalarFunction> $class
     */
    #[DataProvider('functions')]
    public function test_every_function_describes_itself(string $class): void
    {
        ScalarFunctionContract::describesItself(self::instance($class), []);
    }

    /**
     * @param class-string<ScalarFunction> $class
     */
    #[DataProvider('functions')]
    public function test_every_function_is_deterministic(string $class): void
    {
        ScalarFunctionContract::declaresDeterminism(self::instance($class), true);
    }

    /**
     * @param class-string<ScalarFunction> $class
     */
    #[DataProvider('functions')]
    public function test_every_function_evaluates_on_the_configured_backend(string $class): void
    {
        ScalarFunctionContract::evaluatesOnTheConfiguredBackend(
            self::instance($class),
            self::instance($class),
            self::instance($class),
        );
    }
}
