<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Functions;

use Flow\ETL\Foundation\Exception\RequiredPHPVersionException;
use Flow\ETL\Functions\All;
use Flow\ETL\Functions\Any;
use Flow\ETL\Functions\ArrayGetCollection;
use Flow\ETL\Functions\ArrayPathExists;
use Flow\ETL\Functions\Between;
use Flow\ETL\Functions\CallUserFunc;
use Flow\ETL\Functions\Coalesce;
use Flow\ETL\Functions\Comparison;
use Flow\ETL\Functions\Contains;
use Flow\ETL\Functions\DOMElementAttributeValue;
use Flow\ETL\Functions\DOMElementNamespaceValue;
use Flow\ETL\Functions\DOMElementNextSibling;
use Flow\ETL\Functions\DOMElementParent;
use Flow\ETL\Functions\DOMElementPreviousSibling;
use Flow\ETL\Functions\DOMElementValue;
use Flow\ETL\Functions\EndsWith;
use Flow\ETL\Functions\Greatest;
use Flow\ETL\Functions\Hash;
use Flow\ETL\Functions\HTMLQuerySelector;
use Flow\ETL\Functions\HTMLQuerySelectorAll;
use Flow\ETL\Functions\IndexOf;
use Flow\ETL\Functions\IndexOfLast;
use Flow\ETL\Functions\IsEmpty;
use Flow\ETL\Functions\IsIn;
use Flow\ETL\Functions\IsNotNumeric;
use Flow\ETL\Functions\IsNumeric;
use Flow\ETL\Functions\IsType;
use Flow\ETL\Functions\IsUtf8;
use Flow\ETL\Functions\JsonEncode;
use Flow\ETL\Functions\Least;
use Flow\ETL\Functions\ListSelect;
use Flow\ETL\Functions\Literal;
use Flow\ETL\Functions\MatchCases;
use Flow\ETL\Functions\Not;
use Flow\ETL\Functions\Optional;
use Flow\ETL\Functions\Reference\ResolvedReference;
use Flow\ETL\Functions\Reference\UnresolvedReference;
use Flow\ETL\Functions\Regex;
use Flow\ETL\Functions\RegexAll;
use Flow\ETL\Functions\RegexMatch;
use Flow\ETL\Functions\RegexMatchAll;
use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\Functions\Size;
use Flow\ETL\Functions\StartsWith;
use Flow\ETL\Functions\StringContainsAny;
use Flow\ETL\Functions\StringEqualsTo;
use Flow\ETL\Functions\StringMatch;
use Flow\ETL\Functions\StructureSelect;
use Flow\ETL\Functions\ToDateTime;
use Flow\ETL\Functions\When;
use Flow\ETL\Functions\XPath;
use Flow\ETL\Tests\Context\ExtractorClasses;
use Flow\ETL\Tests\Context\ScalarFunctionClasses;
use Flow\ETL\Tests\Context\ScalarFunctionContract;
use Flow\ETL\Tests\Context\ScalarFunctionFixtures;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function array_diff;
use function array_intersect;
use function array_keys;
use function array_values;
use function Flow\Types\DSL\type_is_nullable;
use function implode;
use function sort;

final class AllFunctionsDeclareTheirTypeTest extends FlowTestCase
{
    /**
     * The declared nullable set: 23 unconditional + 7 composed + 25 null-propagating predicates.
     * Aggregate counts alone are satisfied by any 50 - these 50 are the ones a worker gets wrong.
     *
     * @return list<class-string<ScalarFunction>>
     */
    public static function declared_nullable_classes(): array
    {
        return [
            // unconditionally nullable (23)
            Optional::class,
            // an opaque callable can always answer null, so its declaration has to admit it
            CallUserFunc::class,
            StringMatch::class,
            Regex::class,
            RegexAll::class,
            XPath::class,
            HTMLQuerySelector::class,
            HTMLQuerySelectorAll::class,
            DOMElementValue::class,
            DOMElementAttributeValue::class,
            DOMElementNamespaceValue::class,
            DOMElementNextSibling::class,
            DOMElementPreviousSibling::class,
            DOMElementParent::class,
            ListSelect::class,
            StructureSelect::class,
            JsonEncode::class,
            Hash::class,
            Size::class,
            IndexOf::class,
            IndexOfLast::class,
            ToDateTime::class,
            ArrayGetCollection::class,
            // composed from children/input (7)
            Coalesce::class,
            When::class,
            MatchCases::class,
            Greatest::class,
            Least::class,
            ResolvedReference::class,
            Literal::class,
            // null-propagating predicates (20)
            All::class,
            Any::class,
            ArrayPathExists::class,
            Between::class,
            Comparison::class,
            Contains::class,
            EndsWith::class,
            IsEmpty::class,
            IsIn::class,
            IsNotNumeric::class,
            IsNumeric::class,
            IsType::class,
            IsUtf8::class,
            Not::class,
            RegexMatch::class,
            RegexMatchAll::class,
            StartsWith::class,
            StringContainsAny::class,
            StringEqualsTo::class,
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
        foreach (ScalarFunctionClasses::declaring(ExtractorClasses::root('core/etl')) as $class) {
            yield $class => [$class];
        }
    }

    public function test_every_discovered_function_has_a_fixture(): void
    {
        $discovered = ScalarFunctionClasses::all(ExtractorClasses::root('core/etl'));
        $fixtures = array_keys(ScalarFunctionFixtures::all());
        sort($fixtures);

        static::assertSame($discovered, $fixtures);
    }

    public function test_the_fixture_schema_is_entirely_nullable(): void
    {
        foreach (ScalarFunctionFixtures::schema()->definitions() as $definition) {
            static::assertTrue($definition->isNullable(), $definition->name());
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

        $expected = ScalarFunctionFixtures::expectedReturns($class);

        static::assertNotNull($expected);
        ScalarFunctionContract::returnsItsDeclaredType($function, $expected);
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

        ScalarFunctionContract::withChildrenRoundTrips($fn);
    }

    public function test_exactly_49_of_the_127_declare_themselves_nullable(): void
    {
        [$nullable, $notNull, $unavailable] = ScalarFunctionFixtures::nullabilitySplit();

        static::assertCount(127, ScalarFunctionClasses::declaring(ExtractorClasses::root('core/etl')));
        static::assertCount(49, self::declared_nullable_classes());
        static::assertCount(
            49,
            [...$nullable, ...array_intersect($unavailable, self::declared_nullable_classes())],
            'nullable set drifted: ' . implode(', ', array_diff($nullable, self::declared_nullable_classes())),
        );
        static::assertCount(78, [...$notNull, ...array_diff($unavailable, self::declared_nullable_classes())]);
    }

    /**
     * @param class-string<ScalarFunction> $class
     */
    #[DataProvider('declared_nullable_provider')]
    public function test_the_nullable_set_is_exactly_the_declared_49(string $class): void
    {
        try {
            $function = ScalarFunctionFixtures::instance($class);
        } catch (RequiredPHPVersionException $e) {
            static::markTestSkipped($e->getMessage());
        }

        $expected = ScalarFunctionFixtures::expectedReturns($class);

        static::assertNotNull($expected);
        static::assertTrue(type_is_nullable($expected));
        ScalarFunctionContract::returnsItsDeclaredType($function, $expected);
    }

    public function test_the_not_null_set_is_the_complement(): void
    {
        [$nullable, $notNull, $unavailable] = ScalarFunctionFixtures::nullabilitySplit();

        $expectedNotNull = array_values(array_diff(
            ScalarFunctionClasses::declaring(ExtractorClasses::root('core/etl')),
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
