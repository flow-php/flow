<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use ErrorException;
use Flow\ETL\Adapter\Excel\Function\IsValidExcelSheetName;
use Flow\ETL\Exception\RequiredPHPVersionException;
use Flow\ETL\Function;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Row\ResolvedReference;
use Flow\ETL\Row\UnresolvedReference;
use Flow\ETL\Tests\Context\ScalarFunctionClasses;
use Flow\ETL\Tests\Context\ScalarFunctionFixtures;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use Throwable;

use function array_diff;
use function array_intersect;
use function array_keys;
use function array_values;
use function count;
use function Flow\ETL\DSL\definition_from_type;
use function Flow\Types\DSL\type_is_nullable;
use function implode;
use function restore_error_handler;
use function set_error_handler;
use function sort;

final class AllFunctionsDeclareTheirTypeTest extends FlowTestCase
{
    /**
     * The declared nullable set: 23 unconditional + 7 composed + 25 null-propagating predicates.
     * Aggregate counts alone are satisfied by any 55 - these 55 are the ones a worker gets wrong.
     *
     * @return list<class-string<ScalarFunction>>
     */
    public static function declared_nullable_classes(): array
    {
        return [
            // unconditionally nullable (23)
            Function\Optional::class,
            // an opaque callable can always answer null, so its declaration has to admit it
            Function\CallUserFunc::class,
            Function\StringMatch::class,
            Function\Regex::class,
            Function\RegexAll::class,
            Function\XPath::class,
            Function\HTMLQuerySelector::class,
            Function\HTMLQuerySelectorAll::class,
            Function\DOMElementValue::class,
            Function\DOMElementAttributeValue::class,
            Function\DOMElementNamespaceValue::class,
            Function\DOMElementNextSibling::class,
            Function\DOMElementPreviousSibling::class,
            Function\DOMElementParent::class,
            Function\ListSelect::class,
            Function\StructureSelect::class,
            Function\JsonEncode::class,
            Function\Hash::class,
            Function\Size::class,
            Function\IndexOf::class,
            Function\IndexOfLast::class,
            Function\ToDateTime::class,
            Function\ArrayGetCollection::class,
            // composed from children/input (7)
            Function\Coalesce::class,
            Function\When::class,
            Function\MatchCases::class,
            Function\Greatest::class,
            Function\Least::class,
            ResolvedReference::class,
            Function\Literal::class,
            // null-propagating predicates (25)
            Function\All::class,
            Function\Any::class,
            Function\ArrayPathExists::class,
            Function\Between::class,
            Function\Contains::class,
            Function\EndsWith::class,
            Function\Equals::class,
            Function\GreaterThan::class,
            Function\GreaterThanEqual::class,
            Function\IsEmpty::class,
            Function\IsIn::class,
            Function\IsNotNumeric::class,
            Function\IsNumeric::class,
            Function\IsType::class,
            Function\IsUtf8::class,
            IsValidExcelSheetName::class,
            Function\LessThan::class,
            Function\LessThanEqual::class,
            Function\Not::class,
            Function\NotEquals::class,
            Function\RegexMatch::class,
            Function\RegexMatchAll::class,
            Function\StartsWith::class,
            Function\StringContainsAny::class,
            Function\StringEqualsTo::class,
        ];
    }

    /**
     * @return Generator<string, array{class-string<ScalarFunction>}>
     */
    public static function declared_nullable_provider(): Generator
    {
        foreach (self::declared_nullable_classes() as $class) {
            yield $class => [$class];
        }
    }

    /**
     * @return Generator<string, array{class-string<ScalarFunction>}>
     */
    public static function declaring_classes_provider(): Generator
    {
        foreach (ScalarFunctionClasses::declaring() as $class) {
            yield $class => [$class];
        }
    }

    public function test_every_discovered_function_has_a_fixture(): void
    {
        $discovered = ScalarFunctionClasses::all();
        $fixtures = array_keys(ScalarFunctionFixtures::all());
        sort($fixtures);

        static::assertSame($discovered, $fixtures);
    }

    public function test_the_fixture_schema_is_entirely_nullable(): void
    {
        foreach (ScalarFunctionFixtures::schema()->definitions() as $definition) {
            static::assertTrue($definition->isNullable(), $definition->entry()->name());
        }
    }

    /**
     * @param class-string<ScalarFunction> $class
     */
    #[DataProvider('declaring_classes_provider')]
    public function test_every_function_answers_returns_with_a_column_representable_type(string $class): void
    {
        try {
            $function = ScalarFunctionFixtures::instance($class);
        } catch (RequiredPHPVersionException $e) {
            static::markTestSkipped($e->getMessage());
        }

        $resolved = (new ReferenceResolver())->resolve($function, ScalarFunctionFixtures::schema());

        $returns = $resolved->returns();

        $expected = ScalarFunctionFixtures::expectedReturns($class);

        static::assertNotNull($expected);
        static::assertSame($expected->toString(), $returns->toString());
        static::assertNotNull(definition_from_type('c', $returns));
    }

    /**
     * @param class-string<ScalarFunction> $class
     */
    #[DataProvider('declaring_classes_provider')]
    public function test_with_children_round_trips(string $class): void
    {
        try {
            $fn = ScalarFunctionFixtures::instance($class);
        } catch (RequiredPHPVersionException $e) {
            static::markTestSkipped($e->getMessage());
        }

        $children = $fn->children();

        static::assertSame($children, $fn->withChildren($children)->children());

        if (count($children) === 0) {
            return;
        }

        set_error_handler(static fn(int $errno, string $errstr) => throw new ErrorException($errstr));

        try {
            $rebuilt = $fn->withChildren([]);
        } catch (Throwable) {
            return;
        } finally {
            restore_error_handler();
        }

        // An arity change must never go unnoticed: an empty rebuild may be constructible for a
        // variadic node, but it must not still report the original arity.
        static::assertNotSame(count($children), count($rebuilt->children()));
    }

    public function test_exactly_55_of_the_134_declare_themselves_nullable(): void
    {
        [$nullable, $notNull, $unavailable] = ScalarFunctionFixtures::nullabilitySplit();

        static::assertCount(134, ScalarFunctionClasses::declaring());
        static::assertCount(55, self::declared_nullable_classes());
        static::assertCount(
            55,
            [...$nullable, ...array_intersect($unavailable, self::declared_nullable_classes())],
            'nullable set drifted: ' . implode(', ', array_diff($nullable, self::declared_nullable_classes())),
        );
        static::assertCount(79, [...$notNull, ...array_diff($unavailable, self::declared_nullable_classes())]);
    }

    /**
     * @param class-string<ScalarFunction> $class
     */
    #[DataProvider('declared_nullable_provider')]
    public function test_the_nullable_set_is_exactly_the_declared_55(string $class): void
    {
        try {
            $function = ScalarFunctionFixtures::instance($class);
        } catch (RequiredPHPVersionException $e) {
            static::markTestSkipped($e->getMessage());
        }

        $resolved = (new ReferenceResolver())->resolve($function, ScalarFunctionFixtures::schema());

        $returns = $resolved->returns();

        static::assertTrue(type_is_nullable($returns));
        static::assertSame(ScalarFunctionFixtures::expectedReturns($class)?->toString(), $returns->toString());
    }

    public function test_the_not_null_set_is_the_complement(): void
    {
        [$nullable, $notNull, $unavailable] = ScalarFunctionFixtures::nullabilitySplit();

        $expectedNotNull = array_values(array_diff(
            ScalarFunctionClasses::declaring(),
            self::declared_nullable_classes(),
            $unavailable,
        ));
        sort($expectedNotNull);
        sort($notNull);

        static::assertSame($expectedNotNull, $notNull);

        $expectedNullable = array_values(array_diff(self::declared_nullable_classes(), $unavailable));
        sort($expectedNullable);
        sort($nullable);

        static::assertSame($expectedNullable, $nullable);
    }

    public function test_an_unresolved_reference_refuses_to_describe_itself(): void
    {
        $this->expectExceptionMessage('Invalid call to returns() on unresolved reference');

        (new UnresolvedReference('a'))->returns();
    }
}
