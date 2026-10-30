<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Functions;

use Flow\ETL\Foundation\Exception\RequiredPHPVersionException;
use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\Tests\Context\ScalarFunctionContract;
use Flow\ETL\Tests\Context\ScalarFunctionFixtures;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function array_keys;

final class AllFunctionsEvaluateOnTheConfiguredBackendTest extends FlowTestCase
{
    /**
     * @return Generator<string, array{class-string<ScalarFunction>}>
     */
    public static function functions(): Generator
    {
        foreach (array_keys(ScalarFunctionFixtures::all()) as $class) {
            yield $class => [$class];
        }
    }

    /**
     * @param class-string<ScalarFunction> $class
     */
    #[DataProvider('functions')]
    public function test_a_function_gives_the_same_values_and_a_column_of_the_configured_backend(string $class): void
    {
        try {
            // a fresh function per run: a function may keep what it evaluated (Literal keeps its column)
            $onPhp = ScalarFunctionFixtures::instance($class);
            $onAdaptive = ScalarFunctionFixtures::instance($class);
            $adaptiveUnderPhp = ScalarFunctionFixtures::instance($class);
        } catch (RequiredPHPVersionException $e) {
            static::markTestSkipped($e->getMessage());
        }

        ScalarFunctionContract::evaluatesOnTheConfiguredBackend($onPhp, $onAdaptive, $adaptiveUnderPhp);
    }
}
