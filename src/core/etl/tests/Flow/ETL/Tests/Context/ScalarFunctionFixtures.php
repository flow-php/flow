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
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_is_nullable;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

/**
 * One constructable instance per ScalarFunction class, built against schema() below. The map is
 * explicit rather than reflected: derived declarations need type-correct operands (Plus needs
 * numeric columns, OnEach a list), which no placeholder heuristic can supply.
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

            /** @var ScalarFunction $resolved */
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
        return self::all()[$class]();
    }

    /**
     * @return array<class-string<ScalarFunction>, callable(): ScalarFunction>
     */
    public static function all(): array
    {
        return [
            Function\All::class => static fn(): ScalarFunction => new Function\All(
                ref('boolean'),
                ref('integer')->isNull(),
            ),
            Function\Any::class => static fn(): ScalarFunction => new Function\Any(
                ref('boolean'),
                ref('integer')->isNull(),
            ),
            Function\Append::class => static fn(): ScalarFunction => new Function\Append(ref('string'), lit('x')),
            Function\ArrayExpand::class => static fn(): ScalarFunction => new Function\ArrayExpand(
                ref('list'),
                ArrayExpand::VALUES,
            ),
            Function\ArrayFilter::class => static fn(): ScalarFunction => new Function\ArrayFilter(
                ref('list'),
                lit(null),
            ),
            Function\ArrayGet::class => static fn(): ScalarFunction => new Function\ArrayGet(ref('structure'), 'field'),
            Function\ArrayGetCollection::class => static fn(): ScalarFunction => new Function\ArrayGetCollection(
                ref('list'),
                lit(['field']),
            ),
            Function\ArrayKeep::class => static fn(): ScalarFunction => new Function\ArrayKeep(ref('list'), lit(null)),
            Function\ArrayKeyRename::class => static fn(): ScalarFunction => new Function\ArrayKeyRename(
                ref('json'),
                'a',
                'b',
            ),
            Function\ArrayKeys::class => static fn(): ScalarFunction => new Function\ArrayKeys(ref('map')),
            Function\ArrayKeysStyleConvert::class => static fn(): ScalarFunction => new Function\ArrayKeysStyleConvert(
                ref('structure'),
                StringStyles::SNAKE,
            ),
            Function\ArrayMerge::class => static fn(): ScalarFunction => new Function\ArrayMerge(
                ref('list'),
                ref('list'),
            ),
            Function\ArrayMergeCollection::class =>
                static fn(): ScalarFunction => new Function\ArrayMergeCollection(ref('list_of_lists')),
            Function\ArrayPathExists::class => static fn(): ScalarFunction => new Function\ArrayPathExists(
                ref('json'),
                lit('a'),
            ),
            Function\ArrayReverse::class => static fn(): ScalarFunction => new Function\ArrayReverse(
                ref('list'),
                false,
            ),
            Function\ArraySort::class => static fn(): ScalarFunction => new Function\ArraySort(
                ref('list'),
                Sort::sort,
                null,
                false,
            ),
            Function\ArrayUnpack::class => static fn(): ScalarFunction => new Function\ArrayUnpack(ref('json')),
            Function\ArrayValues::class => static fn(): ScalarFunction => new Function\ArrayValues(ref('list')),
            Function\Ascii::class => static fn(): ScalarFunction => new Function\Ascii(ref('string')),
            Function\Between::class => static fn(): ScalarFunction => new Function\Between(
                ref('integer'),
                lit(1),
                lit(5),
            ),
            Function\BinaryLength::class => static fn(): ScalarFunction => new Function\BinaryLength(ref('string')),
            Function\CallUserFunc::class => static fn(): ScalarFunction => new Function\CallUserFunc(
                lit('strtoupper'),
                [ref('string')],
                type_string(),
            ),
            Function\Capitalize::class => static fn(): ScalarFunction => new Function\Capitalize(ref('string')),
            Function\Cast::class => static fn(): ScalarFunction => new Function\Cast(ref('string'), 'int'),
            Function\Chunk::class => static fn(): ScalarFunction => new Function\Chunk(ref('string'), lit(2)),
            Function\Coalesce::class => static fn(): ScalarFunction => new Function\Coalesce(
                ref('string'),
                ref('string'),
            ),
            Function\CodePointLength::class => static fn(): ScalarFunction => new Function\CodePointLength(ref(
                'string',
            )),
            Function\CollapseWhitespace::class => static fn(): ScalarFunction => new Function\CollapseWhitespace(ref(
                'string',
            )),
            Function\Combine::class => static fn(): ScalarFunction => new Function\Combine(ref('list'), ref('list')),
            Function\Concat::class => static fn(): ScalarFunction => new Function\Concat(ref('string'), lit('x')),
            Function\ConcatWithSeparator::class => static fn(): ScalarFunction => new Function\ConcatWithSeparator(
                lit(','),
                ref('string'),
            ),
            Function\Contains::class => static fn(): ScalarFunction => new Function\Contains(ref('string'), lit('a')),
            Function\DateTimeFormat::class => static fn(): ScalarFunction => new Function\DateTimeFormat(
                ref('datetime'),
                lit('Y-m-d'),
            ),
            Function\DOMElementAttributesCount::class =>
                static fn(): ScalarFunction => new Function\DOMElementAttributesCount(ref('xml_element')),
            Function\DOMElementAttributeValue::class =>
                static fn(): ScalarFunction => new Function\DOMElementAttributeValue(ref('xml_element'), lit('attr')),
            Function\DOMElementNamespaceValue::class =>
                static fn(): ScalarFunction => new Function\DOMElementNamespaceValue(ref('xml_element'), lit(null)),
            Function\DOMElementNextSibling::class =>
                static fn(): ScalarFunction => new Function\DOMElementNextSibling(ref('xml_element')),
            Function\DOMElementParent::class => static fn(): ScalarFunction => new Function\DOMElementParent(ref(
                'xml_element',
            )),
            Function\DOMElementPreviousSibling::class =>
                static fn(): ScalarFunction => new Function\DOMElementPreviousSibling(ref('xml_element')),
            Function\DOMElementValue::class => static fn(): ScalarFunction => new Function\DOMElementValue(ref(
                'xml_element',
            )),
            Function\Divide::class => static fn(): ScalarFunction => new Function\Divide(
                ref('integer'),
                ref('integer'),
            ),
            Function\EndsWith::class => static fn(): ScalarFunction => new Function\EndsWith(ref('string'), lit('a')),
            Function\EnsureEnd::class => static fn(): ScalarFunction => new Function\EnsureEnd(ref('string'), lit('x')),
            Function\EnsureStart::class => static fn(): ScalarFunction => new Function\EnsureStart(
                ref('string'),
                lit('x'),
            ),
            Function\EnumName::class => static fn(): ScalarFunction => new Function\EnumName(ref('enum')),
            Function\EnumValue::class => static fn(): ScalarFunction => new Function\EnumValue(ref('enum')),
            Function\Equals::class => static fn(): ScalarFunction => new Function\Equals(
                ref('integer'),
                ref('integer'),
            ),
            Function\Exists::class => static fn(): ScalarFunction => new Function\Exists(ref('string')),
            Function\GreaterThan::class => static fn(): ScalarFunction => new Function\GreaterThan(
                ref('integer'),
                ref('integer'),
            ),
            Function\GreaterThanEqual::class => static fn(): ScalarFunction => new Function\GreaterThanEqual(
                ref('integer'),
                ref('integer'),
            ),
            Function\Greatest::class => static fn(): ScalarFunction => new Function\Greatest([
                ref('integer'),
                ref('integer'),
            ]),
            Function\Hash::class => static fn(): ScalarFunction => new Function\Hash(ref('string')),
            Function\HTMLQuerySelector::class => static fn(): ScalarFunction => new Function\HTMLQuerySelector(
                ref('html'),
                lit('div'),
            ),
            Function\HTMLQuerySelectorAll::class => static fn(): ScalarFunction => new Function\HTMLQuerySelectorAll(
                ref('html'),
                lit('div'),
            ),
            Function\IndexOf::class => static fn(): ScalarFunction => new Function\IndexOf(ref('string'), lit('a')),
            Function\IndexOfLast::class => static fn(): ScalarFunction => new Function\IndexOfLast(
                ref('string'),
                lit('a'),
            ),
            Function\IsEmpty::class => static fn(): ScalarFunction => new Function\IsEmpty(ref('string')),
            Function\IsIn::class => static fn(): ScalarFunction => new Function\IsIn(ref('list'), lit('a')),
            Function\IsNotNull::class => static fn(): ScalarFunction => new Function\IsNotNull(ref('string')),
            Function\IsNotNumeric::class => static fn(): ScalarFunction => new Function\IsNotNumeric(ref('string')),
            Function\IsNull::class => static fn(): ScalarFunction => new Function\IsNull(ref('string')),
            Function\IsNumeric::class => static fn(): ScalarFunction => new Function\IsNumeric(ref('string')),
            Function\IsType::class => static fn(): ScalarFunction => new Function\IsType(ref('string'), type_string()),
            Function\IsUtf8::class => static fn(): ScalarFunction => new Function\IsUtf8(ref('string')),
            IsValidExcelSheetName::class => static fn(): ScalarFunction => new IsValidExcelSheetName(ref('string')),
            Function\JsonDecode::class => static fn(): ScalarFunction => new Function\JsonDecode(ref('string')),
            Function\JsonEncode::class => static fn(): ScalarFunction => new Function\JsonEncode(ref('json')),
            Function\Least::class => static fn(): ScalarFunction => new Function\Least([
                ref('integer'),
                ref('integer'),
            ]),
            Function\LessThan::class => static fn(): ScalarFunction => new Function\LessThan(
                ref('integer'),
                ref('integer'),
            ),
            Function\LessThanEqual::class => static fn(): ScalarFunction => new Function\LessThanEqual(
                ref('integer'),
                ref('integer'),
            ),
            Function\ListSelect::class => static fn(): ScalarFunction => new Function\ListSelect(
                ref('list_of_structs'),
                'field',
            ),
            Function\Literal::class => static fn(): ScalarFunction => new Function\Literal(null),
            Function\MatchCases::class => static fn(): ScalarFunction => new Function\MatchCases([new MatchCondition(
                ref('boolean'),
                ref('string'),
            )]),
            MatchCondition::class => static fn(): ScalarFunction => new MatchCondition(ref('boolean'), lit('x')),
            Function\Minus::class => static fn(): ScalarFunction => new Function\Minus(ref('integer'), ref('integer')),
            Function\Mod::class => static fn(): ScalarFunction => new Function\Mod(ref('integer'), ref('integer')),
            Function\ModifyDateTime::class => static fn(): ScalarFunction => new Function\ModifyDateTime(
                ref('datetime'),
                lit('+1 day'),
            ),
            Function\Multiply::class => static fn(): ScalarFunction => new Function\Multiply(
                ref('integer'),
                ref('integer'),
            ),
            Function\Not::class => static fn(): ScalarFunction => new Function\Not(ref('boolean')),
            Function\NotEquals::class => static fn(): ScalarFunction => new Function\NotEquals(
                ref('integer'),
                ref('integer'),
            ),
            Function\NotSame::class => static fn(): ScalarFunction => new Function\NotSame(
                ref('integer'),
                ref('integer'),
            ),
            Function\Now::class => static fn(): ScalarFunction => new Function\Now(),
            Function\NumberFormat::class => static fn(): ScalarFunction => new Function\NumberFormat(
                ref('float'),
                lit(2),
            ),
            Function\OnEach::class => static fn(): ScalarFunction => new Function\OnEach(
                ref('list'),
                ref('element')->upper(),
            ),
            Function\Optional::class => static fn(): ScalarFunction => new Function\Optional(ref('string')->upper()),
            Function\Plus::class => static fn(): ScalarFunction => new Function\Plus(ref('integer'), ref('integer')),
            Function\Power::class => static fn(): ScalarFunction => new Function\Power(ref('integer'), ref('integer')),
            Function\Prepend::class => static fn(): ScalarFunction => new Function\Prepend(ref('string'), lit('x')),
            Function\RandomString::class => static fn(): ScalarFunction => new Function\RandomString(lit(5)),
            Function\Regex::class => static fn(): ScalarFunction => new Function\Regex(lit('/a/'), ref('string')),
            Function\RegexAll::class => static fn(): ScalarFunction => new Function\RegexAll(lit('/a/'), ref('string')),
            Function\RegexMatch::class => static fn(): ScalarFunction => new Function\RegexMatch(
                lit('/a/'),
                ref('string'),
            ),
            Function\RegexMatchAll::class => static fn(): ScalarFunction => new Function\RegexMatchAll(
                lit('/a/'),
                ref('string'),
            ),
            Function\RegexReplace::class => static fn(): ScalarFunction => new Function\RegexReplace(
                lit('/a/'),
                lit('b'),
                ref('string'),
            ),
            Function\Repeat::class => static fn(): ScalarFunction => new Function\Repeat(ref('string'), lit(2)),
            ResolvedReference::class => static fn(): ScalarFunction => new ResolvedReference(
                'string',
                type_optional(type_string()),
            ),
            Function\Reverse::class => static fn(): ScalarFunction => new Function\Reverse(ref('string')),
            Function\Round::class => static fn(): ScalarFunction => new Function\Round(ref('float')),
            Function\Same::class => static fn(): ScalarFunction => new Function\Same(ref('integer'), ref('integer')),
            Function\Sanitize::class => static fn(): ScalarFunction => new Function\Sanitize(ref('string'), lit('*')),
            Function\Size::class => static fn(): ScalarFunction => new Function\Size(ref('list')),
            Function\Slug::class => static fn(): ScalarFunction => new Function\Slug(ref('string')),
            Function\Split::class => static fn(): ScalarFunction => new Function\Split(ref('string'), lit(',')),
            Function\Sprintf::class => static fn(): ScalarFunction => new Function\Sprintf(lit('%s'), ref('string')),
            Function\StartsWith::class => static fn(): ScalarFunction => new Function\StartsWith(
                ref('string'),
                lit('a'),
            ),
            Function\StringAfter::class => static fn(): ScalarFunction => new Function\StringAfter(
                ref('string'),
                lit('a'),
            ),
            Function\StringAfterLast::class => static fn(): ScalarFunction => new Function\StringAfterLast(
                ref('string'),
                lit('a'),
            ),
            Function\StringBefore::class => static fn(): ScalarFunction => new Function\StringBefore(
                ref('string'),
                lit('a'),
            ),
            Function\StringBeforeLast::class => static fn(): ScalarFunction => new Function\StringBeforeLast(
                ref('string'),
                lit('a'),
            ),
            Function\StringContainsAny::class => static fn(): ScalarFunction => new Function\StringContainsAny(
                ref('string'),
                lit(['a']),
            ),
            Function\StringEqualsTo::class => static fn(): ScalarFunction => new Function\StringEqualsTo(
                ref('string'),
                lit('x'),
            ),
            Function\StringFold::class => static fn(): ScalarFunction => new Function\StringFold(ref('string')),
            Function\StringMatch::class => static fn(): ScalarFunction => new Function\StringMatch(
                ref('string'),
                lit('/a/'),
            ),
            Function\StringMatchAll::class => static fn(): ScalarFunction => new Function\StringMatchAll(
                ref('string'),
                lit('/a/'),
            ),
            Function\StringNormalize::class => static fn(): ScalarFunction => new Function\StringNormalize(ref(
                'string',
            )),
            Function\StringStyle::class => static fn(): ScalarFunction => new Function\StringStyle(
                ref('string'),
                StringStyles::SNAKE,
            ),
            Function\StringTitle::class => static fn(): ScalarFunction => new Function\StringTitle(ref('string')),
            Function\StringWidth::class => static fn(): ScalarFunction => new Function\StringWidth(ref('string')),
            Function\StrPad::class => static fn(): ScalarFunction => new Function\StrPad(ref('string'), lit(5)),
            Function\StrReplace::class => static fn(): ScalarFunction => new Function\StrReplace(
                ref('string'),
                lit('a'),
                lit('b'),
            ),
            Function\StructureSelect::class => static fn(): ScalarFunction => new Function\StructureSelect(
                ref('structure'),
                'field',
            ),
            Function\ToDate::class => static fn(): ScalarFunction => new Function\ToDate(ref('string'), lit('Y-m-d')),
            Function\ToDateTime::class => static fn(): ScalarFunction => new Function\ToDateTime(
                ref('string'),
                lit('Y-m-d'),
            ),
            Function\ToLower::class => static fn(): ScalarFunction => new Function\ToLower(ref('string')),
            Function\ToTimeZone::class => static fn(): ScalarFunction => new Function\ToTimeZone(
                ref('datetime'),
                lit('UTC'),
            ),
            Function\ToUpper::class => static fn(): ScalarFunction => new Function\ToUpper(ref('string')),
            Function\Trim::class => static fn(): ScalarFunction => new Function\Trim(ref('string')),
            Function\Truncate::class => static fn(): ScalarFunction => new Function\Truncate(ref('string'), lit(2)),
            Function\Ulid::class => static fn(): ScalarFunction => new Function\Ulid(),
            Function\UnicodeLength::class => static fn(): ScalarFunction => new Function\UnicodeLength(ref('string')),
            UnresolvedReference::class => static fn(): ScalarFunction => new UnresolvedReference('string'),
            Function\Uuid::class => static fn(): ScalarFunction => Function\Uuid::uuid4(),
            Function\When::class => static fn(): ScalarFunction => new Function\When(ref('boolean'), lit(1)),
            Function\Wordwrap::class => static fn(): ScalarFunction => new Function\Wordwrap(ref('string'), lit(5)),
            Function\XPath::class => static fn(): ScalarFunction => new Function\XPath(ref('xml'), lit('//a')),
        ];
    }
}
