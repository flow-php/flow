<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\Adapter\Excel\Function\IsValidExcelSheetName;
use Flow\ETL\Exception\RequiredPHPVersionException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Function;
use Flow\ETL\Function\ArrayExpand\ArrayExpand;
use Flow\ETL\Function\ArraySort\Sort;
use Flow\ETL\Function\MatchCases\MatchCondition;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Row\ResolvedReference;
use Flow\ETL\Row\UnresolvedReference;
use Flow\ETL\Schema;
use Flow\ETL\String\StringStyles;
use Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum;
use Flow\Types\Type;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\enum_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\html_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\ETL\DSL\xml_element_schema;
use function Flow\ETL\DSL\xml_schema;
use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_html_element;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_is_nullable;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_uuid;
use function Flow\Types\DSL\type_xml_element;

/**
 * One constructable instance per ScalarFunction class, built against schema() below, paired with
 * the exact Type that class declares over that schema. The map is explicit rather than reflected:
 * derived declarations need type-correct operands (Plus needs numeric columns, OnEach a list),
 * which no placeholder heuristic can supply.
 *
 * @type Fixture = array{factory: callable(): ScalarFunction, returns: null|Type<mixed>}
 */
final class ScalarFunctionFixtures
{
    /**
     * Every declaring class resolved against schema() and split by type_is_nullable() of its
     * returns() answer; the one designed SchemaNotDerivableException refusal counts as NOT NULL.
     *
     * @return array{list<class-string<ScalarFunction>>, list<class-string<ScalarFunction>>, list<class-string<ScalarFunction>>}
     */
    public static function nullabilitySplit(): array
    {
        $nullable = [];
        $notNull = [];
        $unavailable = [];

        foreach (ScalarFunctionClasses::declaring() as $class) {
            try {
                $function = self::instance($class);
            } catch (RequiredPHPVersionException) {
                $unavailable[] = $class;

                continue;
            }

            $resolved = (new ReferenceResolver())->resolve($function, self::schema());

            try {
                $type = $resolved->returns();
            } catch (SchemaNotDerivableException) {
                $notNull[] = $class;

                continue;
            }

            if (type_is_nullable($type)) {
                $nullable[] = $class;
            } else {
                $notNull[] = $class;
            }
        }

        return [$nullable, $notNull, $unavailable];
    }

    /**
     * Every column is nullable on purpose: 25 of the nullable declarations are predicates whose
     * nullability is OR-ed from their operands, and ResolvedReference reads its column's
     * Definition - over a NOT NULL schema they would all answer bare types and the split would
     * fail for a reason unrelated to the code under test.
     */
    public static function schema(): Schema
    {
        return schema(
            str_schema('string', true),
            int_schema('integer', true),
            float_schema('float', true),
            bool_schema('boolean', true),
            datetime_schema('datetime', true),
            json_schema('json', true),
            xml_schema('xml', true),
            xml_element_schema('xml_element', true),
            html_schema('html', true),
            enum_schema('enum', BackedStringEnum::class, true),
            list_schema('list', type_list(type_string()), true),
            list_schema('list_of_lists', type_list(type_list(type_string())), true),
            list_schema('list_of_structs', type_list(type_structure(['field' => type_string()])), true),
            map_schema('map', type_map(type_string(), type_integer()), true),
            structure_schema('structure', type_structure(['field' => type_integer()]), true),
        );
    }

    /**
     * @param class-string<ScalarFunction> $class
     *
     * @throws RequiredPHPVersionException when the class needs a newer PHP than the runtime
     */
    public static function instance(string $class): ScalarFunction
    {
        return self::all()[$class]['factory']();
    }

    /**
     * The exact Type the class declares once resolved against schema(); null for the two classes
     * without a declaration - ArrayUnpack (the designed SchemaNotDerivableException refusal) and
     * UnresolvedReference (resolves into ResolvedReference instead of declaring anything).
     *
     * @param class-string<ScalarFunction> $class
     *
     * @return null|Type<mixed>
     */
    public static function expectedReturns(string $class): ?Type
    {
        return self::all()[$class]['returns'];
    }

    /**
     * @return array<class-string<ScalarFunction>, Fixture>
     */
    public static function all(): array
    {
        return [
            Function\All::class => [
                'factory' => static fn(): ScalarFunction => new Function\All(ref('boolean'), ref('integer')->isNull()),
                'returns' => type_optional(type_boolean()),
            ],
            Function\Any::class => [
                'factory' => static fn(): ScalarFunction => new Function\Any(ref('boolean'), ref('integer')->isNull()),
                'returns' => type_optional(type_boolean()),
            ],
            Function\Append::class => [
                'factory' => static fn(): ScalarFunction => new Function\Append(ref('string'), lit('x')),
                'returns' => type_string(),
            ],
            Function\ArrayExpand::class => [
                'factory' => static fn(): ScalarFunction => new Function\ArrayExpand(ref('list'), ArrayExpand::VALUES),
                'returns' => type_string(),
            ],
            Function\ArrayFilter::class => [
                'factory' => static fn(): ScalarFunction => new Function\ArrayFilter(ref('list'), lit(null)),
                'returns' => type_map(type_integer(), type_string()),
            ],
            Function\ArrayGet::class => [
                'factory' => static fn(): ScalarFunction => new Function\ArrayGet(ref('structure'), 'field'),
                'returns' => type_integer(),
            ],
            Function\ArrayGetCollection::class => [
                'factory' => static fn(): ScalarFunction => new Function\ArrayGetCollection(
                    ref('list'),
                    lit(['field']),
                ),
                'returns' => type_optional(type_array()),
            ],
            Function\ArrayKeep::class => [
                'factory' => static fn(): ScalarFunction => new Function\ArrayKeep(ref('list'), lit(null)),
                'returns' => type_map(type_integer(), type_string()),
            ],
            Function\ArrayKeyRename::class => [
                'factory' => static fn(): ScalarFunction => new Function\ArrayKeyRename(ref('json'), 'a', 'b'),
                'returns' => type_array(),
            ],
            Function\ArrayKeys::class => [
                'factory' => static fn(): ScalarFunction => new Function\ArrayKeys(ref('map')),
                'returns' => type_list(type_string()),
            ],
            Function\ArrayKeysStyleConvert::class => [
                'factory' => static fn(): ScalarFunction => new Function\ArrayKeysStyleConvert(
                    ref('structure'),
                    StringStyles::SNAKE,
                ),
                'returns' => type_structure(['field' => type_integer()]),
            ],
            Function\ArrayMerge::class => [
                'factory' => static fn(): ScalarFunction => new Function\ArrayMerge(ref('list'), ref('list')),
                'returns' => type_list(type_string()),
            ],
            Function\ArrayMergeCollection::class => [
                'factory' => static fn(): ScalarFunction => new Function\ArrayMergeCollection(ref('list_of_lists')),
                'returns' => type_list(type_string()),
            ],
            Function\ArrayPathExists::class => [
                'factory' => static fn(): ScalarFunction => new Function\ArrayPathExists(ref('json'), lit('a')),
                'returns' => type_optional(type_boolean()),
            ],
            Function\ArrayReverse::class => [
                'factory' => static fn(): ScalarFunction => new Function\ArrayReverse(ref('list'), false),
                'returns' => type_list(type_string()),
            ],
            Function\ArraySort::class => [
                'factory' => static fn(): ScalarFunction => new Function\ArraySort(
                    ref('list'),
                    Sort::sort,
                    null,
                    false,
                ),
                'returns' => type_list(type_string()),
            ],
            Function\ArrayUnpack::class => [
                'factory' => static fn(): ScalarFunction => new Function\ArrayUnpack(
                    ref('json'),
                    schema(str_schema('a')),
                ),
                'returns' => type_structure(['a' => structure_element('a', type_optional(type_string()))]),
            ],
            Function\ArrayValues::class => [
                'factory' => static fn(): ScalarFunction => new Function\ArrayValues(ref('list')),
                'returns' => type_list(type_string()),
            ],
            Function\Ascii::class => [
                'factory' => static fn(): ScalarFunction => new Function\Ascii(ref('string')),
                'returns' => type_string(),
            ],
            Function\Between::class => [
                'factory' => static fn(): ScalarFunction => new Function\Between(ref('integer'), lit(1), lit(5)),
                'returns' => type_optional(type_boolean()),
            ],
            Function\BinaryLength::class => [
                'factory' => static fn(): ScalarFunction => new Function\BinaryLength(ref('string')),
                'returns' => type_integer(),
            ],
            Function\CallUserFunc::class => [
                'factory' => static fn(): ScalarFunction => new Function\CallUserFunc(
                    lit('strtoupper'),
                    type_string(),
                    [ref('string')],
                ),
                'returns' => type_optional(type_string()),
            ],
            Function\Capitalize::class => [
                'factory' => static fn(): ScalarFunction => new Function\Capitalize(ref('string')),
                'returns' => type_string(),
            ],
            Function\Cast::class => [
                'factory' => static fn(): ScalarFunction => new Function\Cast(ref('string'), 'int'),
                'returns' => type_integer(),
            ],
            Function\Chunk::class => [
                'factory' => static fn(): ScalarFunction => new Function\Chunk(ref('string'), lit(2)),
                'returns' => type_list(type_string()),
            ],
            Function\Coalesce::class => [
                'factory' => static fn(): ScalarFunction => new Function\Coalesce(ref('string'), ref('string')),
                'returns' => type_optional(type_string()),
            ],
            Function\CodePointLength::class => [
                'factory' => static fn(): ScalarFunction => new Function\CodePointLength(ref('string')),
                'returns' => type_integer(),
            ],
            Function\CollapseWhitespace::class => [
                'factory' => static fn(): ScalarFunction => new Function\CollapseWhitespace(ref('string')),
                'returns' => type_string(),
            ],
            Function\Combine::class => [
                'factory' => static fn(): ScalarFunction => new Function\Combine(ref('list'), ref('list')),
                'returns' => type_map(type_string(), type_string()),
            ],
            Function\Concat::class => [
                'factory' => static fn(): ScalarFunction => new Function\Concat(ref('string'), lit('x')),
                'returns' => type_string(),
            ],
            Function\ConcatWithSeparator::class => [
                'factory' => static fn(): ScalarFunction => new Function\ConcatWithSeparator(lit(','), ref('string')),
                'returns' => type_string(),
            ],
            Function\Contains::class => [
                'factory' => static fn(): ScalarFunction => new Function\Contains(ref('string'), lit('a')),
                'returns' => type_optional(type_boolean()),
            ],
            Function\DateTimeFormat::class => [
                'factory' => static fn(): ScalarFunction => new Function\DateTimeFormat(ref('datetime'), lit('Y-m-d')),
                'returns' => type_string(),
            ],
            Function\DOMElementAttributesCount::class => [
                'factory' => static fn(): ScalarFunction => new Function\DOMElementAttributesCount(ref('xml_element')),
                'returns' => type_integer(),
            ],
            Function\DOMElementAttributeValue::class => [
                'factory' => static fn(): ScalarFunction => new Function\DOMElementAttributeValue(
                    ref('xml_element'),
                    lit('attr'),
                ),
                'returns' => type_optional(type_string()),
            ],
            Function\DOMElementNamespaceValue::class => [
                'factory' => static fn(): ScalarFunction => new Function\DOMElementNamespaceValue(
                    ref('xml_element'),
                    lit(null),
                ),
                'returns' => type_optional(type_string()),
            ],
            Function\DOMElementNextSibling::class => [
                'factory' => static fn(): ScalarFunction => new Function\DOMElementNextSibling(ref('xml_element')),
                'returns' => type_optional(type_xml_element()),
            ],
            Function\DOMElementParent::class => [
                'factory' => static fn(): ScalarFunction => new Function\DOMElementParent(ref('xml_element')),
                'returns' => type_optional(type_xml_element()),
            ],
            Function\DOMElementPreviousSibling::class => [
                'factory' => static fn(): ScalarFunction => new Function\DOMElementPreviousSibling(ref('xml_element')),
                'returns' => type_optional(type_xml_element()),
            ],
            Function\DOMElementValue::class => [
                'factory' => static fn(): ScalarFunction => new Function\DOMElementValue(ref('xml_element')),
                'returns' => type_optional(type_string()),
            ],
            Function\Divide::class => [
                'factory' => static fn(): ScalarFunction => new Function\Divide(ref('integer'), ref('integer')),
                'returns' => type_float(),
            ],
            Function\EndsWith::class => [
                'factory' => static fn(): ScalarFunction => new Function\EndsWith(ref('string'), lit('a')),
                'returns' => type_optional(type_boolean()),
            ],
            Function\EnsureEnd::class => [
                'factory' => static fn(): ScalarFunction => new Function\EnsureEnd(ref('string'), lit('x')),
                'returns' => type_string(),
            ],
            Function\EnsureStart::class => [
                'factory' => static fn(): ScalarFunction => new Function\EnsureStart(ref('string'), lit('x')),
                'returns' => type_string(),
            ],
            Function\EnumName::class => [
                'factory' => static fn(): ScalarFunction => new Function\EnumName(ref('enum')),
                'returns' => type_string(),
            ],
            Function\EnumValue::class => [
                'factory' => static fn(): ScalarFunction => new Function\EnumValue(ref('enum')),
                'returns' => type_string(),
            ],
            Function\Equals::class => [
                'factory' => static fn(): ScalarFunction => new Function\Equals(ref('integer'), ref('integer')),
                'returns' => type_optional(type_boolean()),
            ],
            Function\Exists::class => [
                'factory' => static fn(): ScalarFunction => new Function\Exists(ref('string')),
                'returns' => type_boolean(),
            ],
            Function\GreaterThan::class => [
                'factory' => static fn(): ScalarFunction => new Function\GreaterThan(ref('integer'), ref('integer')),
                'returns' => type_optional(type_boolean()),
            ],
            Function\GreaterThanEqual::class => [
                'factory' => static fn(): ScalarFunction => new Function\GreaterThanEqual(
                    ref('integer'),
                    ref('integer'),
                ),
                'returns' => type_optional(type_boolean()),
            ],
            Function\Greatest::class => [
                'factory' => static fn(): ScalarFunction => new Function\Greatest([
                    ref('integer'),
                    ref('integer'),
                ]),
                'returns' => type_optional(type_integer()),
            ],
            Function\Hash::class => [
                'factory' => static fn(): ScalarFunction => new Function\Hash(ref('string')),
                'returns' => type_optional(type_string()),
            ],
            Function\HTMLQuerySelector::class => [
                'factory' => static fn(): ScalarFunction => new Function\HTMLQuerySelector(ref('html'), lit('div')),
                'returns' => type_optional(type_html_element()),
            ],
            Function\HTMLQuerySelectorAll::class => [
                'factory' => static fn(): ScalarFunction => new Function\HTMLQuerySelectorAll(ref('html'), lit('div')),
                'returns' => type_optional(type_list(type_html_element())),
            ],
            Function\IndexOf::class => [
                'factory' => static fn(): ScalarFunction => new Function\IndexOf(ref('string'), lit('a')),
                'returns' => type_optional(type_integer()),
            ],
            Function\IndexOfLast::class => [
                'factory' => static fn(): ScalarFunction => new Function\IndexOfLast(ref('string'), lit('a')),
                'returns' => type_optional(type_integer()),
            ],
            Function\IsEmpty::class => [
                'factory' => static fn(): ScalarFunction => new Function\IsEmpty(ref('string')),
                'returns' => type_optional(type_boolean()),
            ],
            Function\IsIn::class => [
                'factory' => static fn(): ScalarFunction => new Function\IsIn(ref('list'), lit('a')),
                'returns' => type_optional(type_boolean()),
            ],
            Function\IsNotNull::class => [
                'factory' => static fn(): ScalarFunction => new Function\IsNotNull(ref('string')),
                'returns' => type_boolean(),
            ],
            Function\IsNotNumeric::class => [
                'factory' => static fn(): ScalarFunction => new Function\IsNotNumeric(ref('string')),
                'returns' => type_optional(type_boolean()),
            ],
            Function\IsNull::class => [
                'factory' => static fn(): ScalarFunction => new Function\IsNull(ref('string')),
                'returns' => type_boolean(),
            ],
            Function\IsNumeric::class => [
                'factory' => static fn(): ScalarFunction => new Function\IsNumeric(ref('string')),
                'returns' => type_optional(type_boolean()),
            ],
            Function\IsType::class => [
                'factory' => static fn(): ScalarFunction => new Function\IsType(ref('string'), type_string()),
                'returns' => type_optional(type_boolean()),
            ],
            Function\IsUtf8::class => [
                'factory' => static fn(): ScalarFunction => new Function\IsUtf8(ref('string')),
                'returns' => type_optional(type_boolean()),
            ],
            IsValidExcelSheetName::class => [
                'factory' => static fn(): ScalarFunction => new IsValidExcelSheetName(ref('string')),
                'returns' => type_optional(type_boolean()),
            ],
            Function\JsonDecode::class => [
                'factory' => static fn(): ScalarFunction => new Function\JsonDecode(ref('string')),
                'returns' => type_array(),
            ],
            Function\JsonEncode::class => [
                'factory' => static fn(): ScalarFunction => new Function\JsonEncode(ref('json')),
                'returns' => type_optional(type_json()),
            ],
            Function\Least::class => [
                'factory' => static fn(): ScalarFunction => new Function\Least([
                    ref('integer'),
                    ref('integer'),
                ]),
                'returns' => type_optional(type_integer()),
            ],
            Function\LessThan::class => [
                'factory' => static fn(): ScalarFunction => new Function\LessThan(ref('integer'), ref('integer')),
                'returns' => type_optional(type_boolean()),
            ],
            Function\LessThanEqual::class => [
                'factory' => static fn(): ScalarFunction => new Function\LessThanEqual(ref('integer'), ref('integer')),
                'returns' => type_optional(type_boolean()),
            ],
            Function\ListSelect::class => [
                'factory' => static fn(): ScalarFunction => new Function\ListSelect(ref('list_of_structs'), 'field'),
                'returns' => type_optional(type_list(type_structure(['field' => type_string()]))),
            ],
            Function\Literal::class => [
                'factory' => static fn(): ScalarFunction => new Function\Literal(null),
                'returns' => type_optional(type_null()),
            ],
            Function\MatchCases::class => [
                'factory' => static fn(): ScalarFunction => new Function\MatchCases([new MatchCondition(
                    ref('boolean'),
                    ref('string'),
                )]),
                'returns' => type_optional(type_string()),
            ],
            MatchCondition::class => [
                'factory' => static fn(): ScalarFunction => new MatchCondition(ref('boolean'), lit('x')),
                'returns' => type_string(),
            ],
            Function\Minus::class => [
                'factory' => static fn(): ScalarFunction => new Function\Minus(ref('integer'), ref('integer')),
                'returns' => type_integer(),
            ],
            Function\Mod::class => [
                'factory' => static fn(): ScalarFunction => new Function\Mod(ref('integer'), ref('integer')),
                'returns' => type_integer(),
            ],
            Function\ModifyDateTime::class => [
                'factory' => static fn(): ScalarFunction => new Function\ModifyDateTime(ref('datetime'), lit('+1 day')),
                'returns' => type_datetime(),
            ],
            Function\Multiply::class => [
                'factory' => static fn(): ScalarFunction => new Function\Multiply(ref('integer'), ref('integer')),
                'returns' => type_integer(),
            ],
            Function\Not::class => [
                'factory' => static fn(): ScalarFunction => new Function\Not(ref('boolean')),
                'returns' => type_optional(type_boolean()),
            ],
            Function\NotEquals::class => [
                'factory' => static fn(): ScalarFunction => new Function\NotEquals(ref('integer'), ref('integer')),
                'returns' => type_optional(type_boolean()),
            ],
            Function\NotSame::class => [
                'factory' => static fn(): ScalarFunction => new Function\NotSame(ref('integer'), ref('integer')),
                'returns' => type_boolean(),
            ],
            Function\Now::class => [
                'factory' => static fn(): ScalarFunction => new Function\Now(),
                'returns' => type_datetime(),
            ],
            Function\NumberFormat::class => [
                'factory' => static fn(): ScalarFunction => new Function\NumberFormat(ref('float'), lit(2)),
                'returns' => type_string(),
            ],
            Function\OnEach::class => [
                'factory' => static fn(): ScalarFunction => new Function\OnEach(ref('list'), ref('element')->upper()),
                'returns' => type_list(type_string()),
            ],
            Function\Optional::class => [
                'factory' => static fn(): ScalarFunction => new Function\Optional(ref('string')->upper()),
                'returns' => type_optional(type_string()),
            ],
            Function\Plus::class => [
                'factory' => static fn(): ScalarFunction => new Function\Plus(ref('integer'), ref('integer')),
                'returns' => type_integer(),
            ],
            Function\Power::class => [
                'factory' => static fn(): ScalarFunction => new Function\Power(ref('integer'), ref('integer')),
                'returns' => type_integer(),
            ],
            Function\Prepend::class => [
                'factory' => static fn(): ScalarFunction => new Function\Prepend(ref('string'), lit('x')),
                'returns' => type_string(),
            ],
            Function\RandomString::class => [
                'factory' => static fn(): ScalarFunction => new Function\RandomString(lit(5)),
                'returns' => type_string(),
            ],
            Function\Regex::class => [
                'factory' => static fn(): ScalarFunction => new Function\Regex(lit('/a/'), ref('string')),
                'returns' => type_optional(type_array()),
            ],
            Function\RegexAll::class => [
                'factory' => static fn(): ScalarFunction => new Function\RegexAll(lit('/a/'), ref('string')),
                'returns' => type_optional(type_array()),
            ],
            Function\RegexMatch::class => [
                'factory' => static fn(): ScalarFunction => new Function\RegexMatch(lit('/a/'), ref('string')),
                'returns' => type_optional(type_boolean()),
            ],
            Function\RegexMatchAll::class => [
                'factory' => static fn(): ScalarFunction => new Function\RegexMatchAll(lit('/a/'), ref('string')),
                'returns' => type_optional(type_boolean()),
            ],
            Function\RegexReplace::class => [
                'factory' => static fn(): ScalarFunction => new Function\RegexReplace(
                    lit('/a/'),
                    lit('b'),
                    ref('string'),
                ),
                'returns' => type_string(),
            ],
            Function\Repeat::class => [
                'factory' => static fn(): ScalarFunction => new Function\Repeat(ref('string'), lit(2)),
                'returns' => type_string(),
            ],
            ResolvedReference::class => [
                'factory' => static fn(): ScalarFunction => new ResolvedReference(
                    'string',
                    type_optional(type_string()),
                ),
                'returns' => type_optional(type_string()),
            ],
            Function\Reverse::class => [
                'factory' => static fn(): ScalarFunction => new Function\Reverse(ref('string')),
                'returns' => type_string(),
            ],
            Function\Round::class => [
                'factory' => static fn(): ScalarFunction => new Function\Round(ref('float')),
                'returns' => type_float(),
            ],
            Function\Same::class => [
                'factory' => static fn(): ScalarFunction => new Function\Same(ref('integer'), ref('integer')),
                'returns' => type_boolean(),
            ],
            Function\Sanitize::class => [
                'factory' => static fn(): ScalarFunction => new Function\Sanitize(ref('string'), lit('*')),
                'returns' => type_string(),
            ],
            Function\Size::class => [
                'factory' => static fn(): ScalarFunction => new Function\Size(ref('list')),
                'returns' => type_optional(type_integer()),
            ],
            Function\Slug::class => [
                'factory' => static fn(): ScalarFunction => new Function\Slug(ref('string')),
                'returns' => type_string(),
            ],
            Function\Split::class => [
                'factory' => static fn(): ScalarFunction => new Function\Split(ref('string'), lit(',')),
                'returns' => type_list(type_string()),
            ],
            Function\Sprintf::class => [
                'factory' => static fn(): ScalarFunction => new Function\Sprintf(lit('%s'), ref('string')),
                'returns' => type_string(),
            ],
            Function\StartsWith::class => [
                'factory' => static fn(): ScalarFunction => new Function\StartsWith(ref('string'), lit('a')),
                'returns' => type_optional(type_boolean()),
            ],
            Function\StringAfter::class => [
                'factory' => static fn(): ScalarFunction => new Function\StringAfter(ref('string'), lit('a')),
                'returns' => type_string(),
            ],
            Function\StringAfterLast::class => [
                'factory' => static fn(): ScalarFunction => new Function\StringAfterLast(ref('string'), lit('a')),
                'returns' => type_string(),
            ],
            Function\StringBefore::class => [
                'factory' => static fn(): ScalarFunction => new Function\StringBefore(ref('string'), lit('a')),
                'returns' => type_string(),
            ],
            Function\StringBeforeLast::class => [
                'factory' => static fn(): ScalarFunction => new Function\StringBeforeLast(ref('string'), lit('a')),
                'returns' => type_string(),
            ],
            Function\StringContainsAny::class => [
                'factory' => static fn(): ScalarFunction => new Function\StringContainsAny(ref('string'), lit(['a'])),
                'returns' => type_optional(type_boolean()),
            ],
            Function\StringEqualsTo::class => [
                'factory' => static fn(): ScalarFunction => new Function\StringEqualsTo(ref('string'), lit('x')),
                'returns' => type_optional(type_boolean()),
            ],
            Function\StringFold::class => [
                'factory' => static fn(): ScalarFunction => new Function\StringFold(ref('string')),
                'returns' => type_string(),
            ],
            Function\StringMatch::class => [
                'factory' => static fn(): ScalarFunction => new Function\StringMatch(ref('string'), lit('/a/')),
                'returns' => type_optional(type_array()),
            ],
            Function\StringMatchAll::class => [
                'factory' => static fn(): ScalarFunction => new Function\StringMatchAll(ref('string'), lit('/a/')),
                'returns' => type_array(),
            ],
            Function\StringNormalize::class => [
                'factory' => static fn(): ScalarFunction => new Function\StringNormalize(ref('string')),
                'returns' => type_string(),
            ],
            Function\StringStyle::class => [
                'factory' => static fn(): ScalarFunction => new Function\StringStyle(
                    ref('string'),
                    StringStyles::SNAKE,
                ),
                'returns' => type_string(),
            ],
            Function\StringTitle::class => [
                'factory' => static fn(): ScalarFunction => new Function\StringTitle(ref('string')),
                'returns' => type_string(),
            ],
            Function\StringWidth::class => [
                'factory' => static fn(): ScalarFunction => new Function\StringWidth(ref('string')),
                'returns' => type_integer(),
            ],
            Function\StrPad::class => [
                'factory' => static fn(): ScalarFunction => new Function\StrPad(ref('string'), lit(5)),
                'returns' => type_string(),
            ],
            Function\StrReplace::class => [
                'factory' => static fn(): ScalarFunction => new Function\StrReplace(ref('string'), lit('a'), lit('b')),
                'returns' => type_string(),
            ],
            Function\Structure::class => [
                'factory' => static fn(): ScalarFunction => new Function\Structure([
                    'id' => ref('string'),
                    'quantity' => ref('integer'),
                ]),
                'returns' => type_structure([
                    'id' => type_optional(type_string()),
                    'quantity' => type_optional(type_integer()),
                ]),
            ],
            Function\StructureSelect::class => [
                'factory' => static fn(): ScalarFunction => new Function\StructureSelect(ref('structure'), 'field'),
                'returns' => type_optional(type_structure(['field' => type_integer()])),
            ],
            Function\ToDate::class => [
                'factory' => static fn(): ScalarFunction => new Function\ToDate(ref('string'), lit('Y-m-d')),
                'returns' => type_date(),
            ],
            Function\ToDateTime::class => [
                'factory' => static fn(): ScalarFunction => new Function\ToDateTime(ref('string'), lit('Y-m-d')),
                'returns' => type_optional(type_datetime()),
            ],
            Function\ToLower::class => [
                'factory' => static fn(): ScalarFunction => new Function\ToLower(ref('string')),
                'returns' => type_string(),
            ],
            Function\ToTimeZone::class => [
                'factory' => static fn(): ScalarFunction => new Function\ToTimeZone(ref('datetime'), lit('UTC')),
                'returns' => type_datetime(),
            ],
            Function\ToUpper::class => [
                'factory' => static fn(): ScalarFunction => new Function\ToUpper(ref('string')),
                'returns' => type_string(),
            ],
            Function\Trim::class => [
                'factory' => static fn(): ScalarFunction => new Function\Trim(ref('string')),
                'returns' => type_string(),
            ],
            Function\Truncate::class => [
                'factory' => static fn(): ScalarFunction => new Function\Truncate(ref('string'), lit(2)),
                'returns' => type_string(),
            ],
            Function\Ulid::class => [
                'factory' => static fn(): ScalarFunction => new Function\Ulid(),
                'returns' => type_string(),
            ],
            Function\UnicodeLength::class => [
                'factory' => static fn(): ScalarFunction => new Function\UnicodeLength(ref('string')),
                'returns' => type_integer(),
            ],
            UnresolvedReference::class => [
                'factory' => static fn(): ScalarFunction => new UnresolvedReference('string'),
                'returns' => null,
            ],
            Function\Uuid::class => [
                'factory' => static fn(): ScalarFunction => Function\Uuid::uuid4(),
                'returns' => type_uuid(),
            ],
            Function\When::class => [
                'factory' => static fn(): ScalarFunction => new Function\When(ref('boolean'), lit(1)),
                'returns' => type_optional(type_integer()),
            ],
            Function\Wordwrap::class => [
                'factory' => static fn(): ScalarFunction => new Function\Wordwrap(ref('string'), lit(5)),
                'returns' => type_string(),
            ],
            Function\XPath::class => [
                'factory' => static fn(): ScalarFunction => new Function\XPath(ref('xml'), lit('//a')),
                'returns' => type_optional(type_list(type_xml_element())),
            ],
        ];
    }
}
