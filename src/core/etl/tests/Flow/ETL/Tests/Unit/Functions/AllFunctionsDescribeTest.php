<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Functions;

use Flow\ETL\Foundation\Exception\RequiredPHPVersionException;
use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\Tests\Context\ExtractorClasses;
use Flow\ETL\Tests\Context\ScalarFunctionClasses;
use Flow\ETL\Tests\Context\ScalarFunctionContract;
use Flow\ETL\Tests\Context\ScalarFunctionFixtures;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function sort;

final class AllFunctionsDescribeTest extends FlowTestCase
{
    /**
     * @return Generator<string, array{class-string<ScalarFunction>}>
     */
    public static function functions(): Generator
    {
        foreach (ScalarFunctionClasses::all(ExtractorClasses::root('core/etl')) as $class) {
            yield $class => [$class];
        }
    }

    /**
     * @param class-string<ScalarFunction> $class
     */
    #[DataProvider('functions')]
    public function test_every_function_round_trips(string $class): void
    {
        try {
            $function = ScalarFunctionFixtures::instance($class);
        } catch (RequiredPHPVersionException $e) {
            static::markTestSkipped($e->getMessage());
        }

        ScalarFunctionContract::describesItself($function, self::undescribable());
    }

    public function test_only_dom_literals_refuse_to_describe(): void
    {
        $refusing = [];

        foreach (ScalarFunctionClasses::all(ExtractorClasses::root('core/etl')) as $class) {
            try {
                $function = ScalarFunctionFixtures::instance($class);
            } catch (RequiredPHPVersionException) {
                continue;
            }

            if ($function->describe() === null) {
                $refusing[] = $class;
            }
        }

        $expected = self::undescribable();
        sort($expected);
        sort($refusing);

        static::assertSame($expected, $refusing);
    }

    /**
     * The fixtures whose describe() is null: every one would hold a value LiteralValue has no encoding for (a DOM
     * node). The DOM functions read their node from a column, so no shipped fixture refuses - LiteralTest covers the
     * DOM literal itself.
     *
     * @return list<class-string<ScalarFunction>>
     */
    public static function undescribable(): array
    {
        return [];
    }
}
