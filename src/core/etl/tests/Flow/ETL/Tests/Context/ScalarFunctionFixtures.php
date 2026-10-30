<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use DateTimeImmutable;
use DateTimeZone;
use DOMDocument;
use Flow\ETL\Foundation\Calculator\Rounding;
use Flow\ETL\Foundation\Exception\RequiredPHPVersionException;
use Flow\ETL\Foundation\Exception\SchemaNotDerivableException;
use Flow\ETL\Functions;
use Flow\ETL\Functions\All;
use Flow\ETL\Functions\Any;
use Flow\ETL\Functions\Append;
use Flow\ETL\Functions\ArrayExpand\ArrayExpand;
use Flow\ETL\Functions\ArrayFilter;
use Flow\ETL\Functions\ArrayGet;
use Flow\ETL\Functions\ArrayGetCollection;
use Flow\ETL\Functions\ArrayKeep;
use Flow\ETL\Functions\ArrayKeyRename;
use Flow\ETL\Functions\ArrayKeys;
use Flow\ETL\Functions\ArrayKeysStyleConvert;
use Flow\ETL\Functions\ArrayMerge;
use Flow\ETL\Functions\ArrayMergeCollection;
use Flow\ETL\Functions\ArrayPathExists;
use Flow\ETL\Functions\ArrayReverse;
use Flow\ETL\Functions\ArraySort;
use Flow\ETL\Functions\ArraySort\Sort;
use Flow\ETL\Functions\ArrayUnpack;
use Flow\ETL\Functions\ArrayValues;
use Flow\ETL\Functions\Ascii;
use Flow\ETL\Functions\Between;
use Flow\ETL\Functions\Between\Boundary;
use Flow\ETL\Functions\BinaryLength;
use Flow\ETL\Functions\CallUserFunc;
use Flow\ETL\Functions\Capitalize;
use Flow\ETL\Functions\Cast;
use Flow\ETL\Functions\Chunk;
use Flow\ETL\Functions\Coalesce;
use Flow\ETL\Functions\CodePointLength;
use Flow\ETL\Functions\CollapseWhitespace;
use Flow\ETL\Functions\Combine;
use Flow\ETL\Functions\Comparison;
use Flow\ETL\Functions\Concat;
use Flow\ETL\Functions\ConcatWithSeparator;
use Flow\ETL\Functions\Contains;
use Flow\ETL\Functions\DateTimeFormat;
use Flow\ETL\Functions\Divide;
use Flow\ETL\Functions\DOMElementAttributesCount;
use Flow\ETL\Functions\DOMElementAttributeValue;
use Flow\ETL\Functions\DOMElementNamespaceValue;
use Flow\ETL\Functions\DOMElementNextSibling;
use Flow\ETL\Functions\DOMElementParent;
use Flow\ETL\Functions\DOMElementPreviousSibling;
use Flow\ETL\Functions\DOMElementValue;
use Flow\ETL\Functions\EndsWith;
use Flow\ETL\Functions\EnsureEnd;
use Flow\ETL\Functions\EnsureStart;
use Flow\ETL\Functions\EnumName;
use Flow\ETL\Functions\EnumValue;
use Flow\ETL\Functions\Exists;
use Flow\ETL\Functions\Greatest;
use Flow\ETL\Functions\Hash;
use Flow\ETL\Functions\HTMLQuerySelector;
use Flow\ETL\Functions\HTMLQuerySelectorAll;
use Flow\ETL\Functions\IndexOf;
use Flow\ETL\Functions\IndexOfLast;
use Flow\ETL\Functions\IsEmpty;
use Flow\ETL\Functions\IsIn;
use Flow\ETL\Functions\IsNotNull;
use Flow\ETL\Functions\IsNotNumeric;
use Flow\ETL\Functions\IsNull;
use Flow\ETL\Functions\IsNumeric;
use Flow\ETL\Functions\IsType;
use Flow\ETL\Functions\IsUtf8;
use Flow\ETL\Functions\JsonDecode;
use Flow\ETL\Functions\JsonEncode;
use Flow\ETL\Functions\Least;
use Flow\ETL\Functions\ListSelect;
use Flow\ETL\Functions\Literal;
use Flow\ETL\Functions\MatchCases;
use Flow\ETL\Functions\MatchCases\MatchCondition;
use Flow\ETL\Functions\Minus;
use Flow\ETL\Functions\Mod;
use Flow\ETL\Functions\ModifyDateTime;
use Flow\ETL\Functions\Multiply;
use Flow\ETL\Functions\Not;
use Flow\ETL\Functions\Now;
use Flow\ETL\Functions\NumberFormat;
use Flow\ETL\Functions\OnEach;
use Flow\ETL\Functions\Optional;
use Flow\ETL\Functions\Plus;
use Flow\ETL\Functions\Power;
use Flow\ETL\Functions\Prepend;
use Flow\ETL\Functions\RandomString;
use Flow\ETL\Functions\Reference\ResolvedReference;
use Flow\ETL\Functions\Reference\UnresolvedReference;
use Flow\ETL\Functions\ReferenceResolver;
use Flow\ETL\Functions\Regex;
use Flow\ETL\Functions\RegexAll;
use Flow\ETL\Functions\RegexMatch;
use Flow\ETL\Functions\RegexMatchAll;
use Flow\ETL\Functions\RegexReplace;
use Flow\ETL\Functions\Repeat;
use Flow\ETL\Functions\Reverse;
use Flow\ETL\Functions\Round;
use Flow\ETL\Functions\Sanitize;
use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\Functions\Size;
use Flow\ETL\Functions\Slug;
use Flow\ETL\Functions\Split;
use Flow\ETL\Functions\Sprintf;
use Flow\ETL\Functions\StartsWith;
use Flow\ETL\Functions\String\StringStyles;
use Flow\ETL\Functions\StringAfter;
use Flow\ETL\Functions\StringAfterLast;
use Flow\ETL\Functions\StringBefore;
use Flow\ETL\Functions\StringBeforeLast;
use Flow\ETL\Functions\StringContainsAny;
use Flow\ETL\Functions\StringEqualsTo;
use Flow\ETL\Functions\StringFold;
use Flow\ETL\Functions\StringMatch;
use Flow\ETL\Functions\StringMatchAll;
use Flow\ETL\Functions\StringNormalize;
use Flow\ETL\Functions\StringStyle;
use Flow\ETL\Functions\StringTitle;
use Flow\ETL\Functions\StringWidth;
use Flow\ETL\Functions\StrPad;
use Flow\ETL\Functions\StrReplace;
use Flow\ETL\Functions\Structure;
use Flow\ETL\Functions\StructureSelect;
use Flow\ETL\Functions\ToDate;
use Flow\ETL\Functions\ToDateTime;
use Flow\ETL\Functions\ToLower;
use Flow\ETL\Functions\ToTimeZone;
use Flow\ETL\Functions\ToUpper;
use Flow\ETL\Functions\Trim;
use Flow\ETL\Functions\Trim\Type as TrimType;
use Flow\ETL\Functions\Truncate;
use Flow\ETL\Functions\Ulid;
use Flow\ETL\Functions\UnicodeLength;
use Flow\ETL\Functions\Uuid;
use Flow\ETL\Functions\When;
use Flow\ETL\Functions\Wordwrap;
use Flow\ETL\Functions\XPath;
use Flow\ETL\Kernels\Hash\NativePHPHash;
use Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\Types\Type;
use Flow\Types\Type\Comparison\Operator;
use Normalizer;

use function Flow\ETL\DSL\array_to_rows;
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

use const JSON_BIGINT_AS_STRING;
use const JSON_PRETTY_PRINT;
use const PHP_ROUND_HALF_EVEN;
use const PREG_OFFSET_CAPTURE;
use const PREG_PATTERN_ORDER;
use const PREG_SET_ORDER;
use const STR_PAD_LEFT;

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

        foreach (ScalarFunctionClasses::declaring(ExtractorClasses::root('core/etl')) as $class) {
            try {
                $function = self::instance($class);
            } catch (RequiredPHPVersionException) {
                $unavailable[] = $class;

                continue;
            }

            try {
                $type = (new ReferenceResolver())
                    ->resolve($function, self::schema())
                    ->returns();
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
     * Three rows under schema(): two of values, the last all null. `html` stays null - a document needs PHP 8.4.
     */
    public static function rows(Backend $backend): Rows
    {
        $document = new DOMDocument();
        $document->loadXML('<root><a>1</a><b>2</b></root>');

        return array_to_rows(
            [
                [
                    'string' => 'a',
                    'integer' => 1,
                    'float' => 1.5,
                    'boolean' => true,
                    'datetime' => new DateTimeImmutable('2026-01-02 03:04:05 UTC'),
                    'json' => '{"a":1}',
                    'xml' => '<root><a>1</a></root>',
                    'xml_element' => $document->getElementsByTagName('a')->item(0),
                    'html' => null,
                    'enum' => BackedStringEnum::one,
                    'list' => ['a', 'b'],
                    'list_of_lists' => [['a'], ['b']],
                    'list_of_structs' => [['field' => 'x']],
                    'map' => ['k' => 1],
                    'structure' => ['field' => 1],
                ],
                [
                    'string' => 'b',
                    'integer' => 2,
                    'float' => -0.5,
                    'boolean' => false,
                    'datetime' => new DateTimeImmutable('2026-01-03 00:00:00 UTC'),
                    'json' => '[1,2]',
                    'xml' => '<root><b>2</b></root>',
                    'xml_element' => $document->getElementsByTagName('b')->item(0),
                    'html' => null,
                    'enum' => BackedStringEnum::two,
                    'list' => [],
                    'list_of_lists' => [],
                    'list_of_structs' => [],
                    'map' => [],
                    'structure' => ['field' => 2],
                ],
                [],
            ],
            self::schema(),
            $backend,
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
            All::class => [
                'factory' => static fn(): ScalarFunction => new All(ref('boolean'), ref('integer')->isNull()),
                'returns' => type_optional(type_boolean()),
            ],
            Any::class => [
                'factory' => static fn(): ScalarFunction => new Any(ref('boolean'), ref('integer')->isNull()),
                'returns' => type_optional(type_boolean()),
            ],
            Append::class => [
                'factory' => static fn(): ScalarFunction => new Append(ref('string'), lit('x')),
                'returns' => type_string(),
            ],
            Functions\ArrayExpand::class => [
                'factory' => static fn(): ScalarFunction => new Functions\ArrayExpand(ref('list'), ArrayExpand::VALUES),
                'returns' => type_string(),
            ],
            ArrayFilter::class => [
                'factory' => static fn(): ScalarFunction => new ArrayFilter(ref('list'), lit('x')),
                'returns' => type_map(type_integer(), type_string()),
            ],
            ArrayGet::class => [
                'factory' => static fn(): ScalarFunction => new ArrayGet(ref('structure'), 'field'),
                'returns' => type_integer(),
            ],
            ArrayGetCollection::class => [
                'factory' => static fn(): ScalarFunction => new ArrayGetCollection(ref('list'), lit(['field']), '0'),
                'returns' => type_optional(type_array()),
            ],
            ArrayKeep::class => [
                'factory' => static fn(): ScalarFunction => new ArrayKeep(ref('list'), lit('x')),
                'returns' => type_map(type_integer(), type_string()),
            ],
            ArrayKeyRename::class => [
                'factory' => static fn(): ScalarFunction => new ArrayKeyRename(ref('json'), 'a', 'b'),
                'returns' => type_array(),
            ],
            ArrayKeys::class => [
                'factory' => static fn(): ScalarFunction => new ArrayKeys(ref('map')),
                'returns' => type_list(type_string()),
            ],
            ArrayKeysStyleConvert::class => [
                'factory' => static fn(): ScalarFunction => new ArrayKeysStyleConvert(
                    ref('structure'),
                    StringStyles::SNAKE,
                ),
                'returns' => type_structure(['field' => type_integer()]),
            ],
            ArrayMerge::class => [
                'factory' => static fn(): ScalarFunction => new ArrayMerge(ref('list'), ref('list')),
                'returns' => type_list(type_string()),
            ],
            ArrayMergeCollection::class => [
                'factory' => static fn(): ScalarFunction => new ArrayMergeCollection(ref('list_of_lists')),
                'returns' => type_list(type_string()),
            ],
            ArrayPathExists::class => [
                'factory' => static fn(): ScalarFunction => new ArrayPathExists(ref('json'), lit('a')),
                'returns' => type_optional(type_boolean()),
            ],
            ArrayReverse::class => [
                'factory' => static fn(): ScalarFunction => new ArrayReverse(ref('list'), false),
                'returns' => type_list(type_string()),
            ],
            ArraySort::class => [
                'factory' => static fn(): ScalarFunction => new ArraySort(ref('list'), Sort::sort, null, false),
                'returns' => type_list(type_string()),
            ],
            ArrayUnpack::class => [
                'factory' => static fn(): ScalarFunction => new ArrayUnpack(ref('json'), schema(str_schema('a'))),
                'returns' => type_structure(['a' => structure_element('a', type_optional(type_string()))]),
            ],
            ArrayValues::class => [
                'factory' => static fn(): ScalarFunction => new ArrayValues(ref('list')),
                'returns' => type_list(type_string()),
            ],
            Ascii::class => [
                'factory' => static fn(): ScalarFunction => new Ascii(ref('string')),
                'returns' => type_string(),
            ],
            Between::class => [
                'factory' => static fn(): ScalarFunction => new Between(
                    ref('integer'),
                    lit(1),
                    lit(5),
                    Boundary::EXCLUSIVE,
                ),
                'returns' => type_optional(type_boolean()),
            ],
            BinaryLength::class => [
                'factory' => static fn(): ScalarFunction => new BinaryLength(ref('string')),
                'returns' => type_integer(),
            ],
            CallUserFunc::class => [
                'factory' => static fn(): ScalarFunction => new CallUserFunc(lit('strval'), type_string(), [ref(
                    'string',
                )]),
                'returns' => type_optional(type_string()),
            ],
            Capitalize::class => [
                'factory' => static fn(): ScalarFunction => new Capitalize(ref('string')),
                'returns' => type_string(),
            ],
            Cast::class => [
                'factory' => static fn(): ScalarFunction => new Cast(ref('string'), 'int'),
                'returns' => type_integer(),
            ],
            Chunk::class => [
                'factory' => static fn(): ScalarFunction => new Chunk(ref('string'), lit(2)),
                'returns' => type_list(type_string()),
            ],
            Coalesce::class => [
                'factory' => static fn(): ScalarFunction => new Coalesce(ref('string'), ref('string')),
                'returns' => type_optional(type_string()),
            ],
            CodePointLength::class => [
                'factory' => static fn(): ScalarFunction => new CodePointLength(ref('string')),
                'returns' => type_integer(),
            ],
            CollapseWhitespace::class => [
                'factory' => static fn(): ScalarFunction => new CollapseWhitespace(ref('string')),
                'returns' => type_string(),
            ],
            Combine::class => [
                'factory' => static fn(): ScalarFunction => new Combine(ref('list'), ref('list')),
                'returns' => type_map(type_string(), type_string()),
            ],
            Comparison::class => [
                'factory' => static fn(): ScalarFunction => new Comparison(
                    ref('integer'),
                    Operator::EQUAL,
                    ref('integer'),
                ),
                'returns' => type_optional(type_boolean()),
            ],
            Concat::class => [
                'factory' => static fn(): ScalarFunction => new Concat(ref('string'), lit('x')),
                'returns' => type_string(),
            ],
            ConcatWithSeparator::class => [
                'factory' => static fn(): ScalarFunction => new ConcatWithSeparator(lit(','), ref('string')),
                'returns' => type_string(),
            ],
            Contains::class => [
                'factory' => static fn(): ScalarFunction => new Contains(ref('string'), lit('a')),
                'returns' => type_optional(type_boolean()),
            ],
            DateTimeFormat::class => [
                'factory' => static fn(): ScalarFunction => new DateTimeFormat(ref('datetime'), lit('Y-m-d')),
                'returns' => type_string(),
            ],
            DOMElementAttributesCount::class => [
                'factory' => static fn(): ScalarFunction => new DOMElementAttributesCount(ref('xml_element')),
                'returns' => type_integer(),
            ],
            DOMElementAttributeValue::class => [
                'factory' => static fn(): ScalarFunction => new DOMElementAttributeValue(
                    ref('xml_element'),
                    lit('attr'),
                ),
                'returns' => type_optional(type_string()),
            ],
            DOMElementNamespaceValue::class => [
                'factory' => static fn(): ScalarFunction => new DOMElementNamespaceValue(ref('xml_element'), lit(null)),
                'returns' => type_optional(type_string()),
            ],
            DOMElementNextSibling::class => [
                'factory' => static fn(): ScalarFunction => new DOMElementNextSibling(ref('xml_element')),
                'returns' => type_optional(type_xml_element()),
            ],
            DOMElementParent::class => [
                'factory' => static fn(): ScalarFunction => new DOMElementParent(ref('xml_element')),
                'returns' => type_optional(type_xml_element()),
            ],
            DOMElementPreviousSibling::class => [
                'factory' => static fn(): ScalarFunction => new DOMElementPreviousSibling(ref('xml_element')),
                'returns' => type_optional(type_xml_element()),
            ],
            DOMElementValue::class => [
                'factory' => static fn(): ScalarFunction => new DOMElementValue(ref('xml_element')),
                'returns' => type_optional(type_string()),
            ],
            Divide::class => [
                'factory' => static fn(): ScalarFunction => new Divide(
                    ref('integer'),
                    ref('integer'),
                    3,
                    Rounding::HALF_EVEN,
                ),
                'returns' => type_float(),
            ],
            EndsWith::class => [
                'factory' => static fn(): ScalarFunction => new EndsWith(ref('string'), lit('a')),
                'returns' => type_optional(type_boolean()),
            ],
            EnsureEnd::class => [
                'factory' => static fn(): ScalarFunction => new EnsureEnd(ref('string'), lit('x')),
                'returns' => type_string(),
            ],
            EnsureStart::class => [
                'factory' => static fn(): ScalarFunction => new EnsureStart(ref('string'), lit('x')),
                'returns' => type_string(),
            ],
            EnumName::class => [
                'factory' => static fn(): ScalarFunction => new EnumName(ref('enum')),
                'returns' => type_string(),
            ],
            EnumValue::class => [
                'factory' => static fn(): ScalarFunction => new EnumValue(ref('enum')),
                'returns' => type_string(),
            ],
            Exists::class => [
                'factory' => static fn(): ScalarFunction => new Exists(ref('string')),
                'returns' => type_boolean(),
            ],
            Greatest::class => [
                'factory' => static fn(): ScalarFunction => new Greatest([
                    ref('integer'),
                    ref('integer'),
                ]),
                'returns' => type_optional(type_integer()),
            ],
            Hash::class => [
                'factory' => static fn(): ScalarFunction => new Hash(ref('string'), new NativePHPHash('sha256', true, [
                    'seed' => 3,
                ])),
                'returns' => type_optional(type_string()),
            ],
            HTMLQuerySelector::class => [
                'factory' => static fn(): ScalarFunction => new HTMLQuerySelector(ref('html'), lit('div')),
                'returns' => type_optional(type_html_element()),
            ],
            HTMLQuerySelectorAll::class => [
                'factory' => static fn(): ScalarFunction => new HTMLQuerySelectorAll(ref('html'), lit('div')),
                'returns' => type_optional(type_list(type_html_element())),
            ],
            IndexOf::class => [
                'factory' => static fn(): ScalarFunction => new IndexOf(ref('string'), lit('a'), true, 1),
                'returns' => type_optional(type_integer()),
            ],
            IndexOfLast::class => [
                'factory' => static fn(): ScalarFunction => new IndexOfLast(ref('string'), lit('a'), true, 1),
                'returns' => type_optional(type_integer()),
            ],
            IsEmpty::class => [
                'factory' => static fn(): ScalarFunction => new IsEmpty(ref('string')),
                'returns' => type_optional(type_boolean()),
            ],
            IsIn::class => [
                'factory' => static fn(): ScalarFunction => new IsIn(ref('list'), lit('a')),
                'returns' => type_optional(type_boolean()),
            ],
            IsNotNull::class => [
                'factory' => static fn(): ScalarFunction => new IsNotNull(ref('string')),
                'returns' => type_boolean(),
            ],
            IsNotNumeric::class => [
                'factory' => static fn(): ScalarFunction => new IsNotNumeric(ref('string')),
                'returns' => type_optional(type_boolean()),
            ],
            IsNull::class => [
                'factory' => static fn(): ScalarFunction => new IsNull(ref('string')),
                'returns' => type_boolean(),
            ],
            IsNumeric::class => [
                'factory' => static fn(): ScalarFunction => new IsNumeric(ref('string')),
                'returns' => type_optional(type_boolean()),
            ],
            IsType::class => [
                'factory' => static fn(): ScalarFunction => new IsType(ref('string'), type_string()),
                'returns' => type_optional(type_boolean()),
            ],
            IsUtf8::class => [
                'factory' => static fn(): ScalarFunction => new IsUtf8(ref('string')),
                'returns' => type_optional(type_boolean()),
            ],
            JsonDecode::class => [
                'factory' => static fn(): ScalarFunction => new JsonDecode(ref('string'), JSON_BIGINT_AS_STRING),
                'returns' => type_array(),
            ],
            JsonEncode::class => [
                'factory' => static fn(): ScalarFunction => new JsonEncode(ref('json'), JSON_PRETTY_PRINT),
                'returns' => type_optional(type_json()),
            ],
            Least::class => [
                'factory' => static fn(): ScalarFunction => new Least([
                    ref('integer'),
                    ref('integer'),
                ]),
                'returns' => type_optional(type_integer()),
            ],
            ListSelect::class => [
                'factory' => static fn(): ScalarFunction => new ListSelect(ref('list_of_structs'), 'field'),
                'returns' => type_optional(type_list(type_structure(['field' => type_string()]))),
            ],
            Literal::class => [
                'factory' => static fn(): ScalarFunction => new Literal(null),
                'returns' => type_optional(type_null()),
            ],
            MatchCases::class => [
                'factory' => static fn(): ScalarFunction => new MatchCases([new MatchCondition(
                    ref('boolean'),
                    ref('string'),
                )], lit('none')),
                'returns' => type_optional(type_string()),
            ],
            MatchCondition::class => [
                'factory' => static fn(): ScalarFunction => new MatchCondition(ref('boolean'), lit('x')),
                'returns' => type_string(),
            ],
            Minus::class => [
                'factory' => static fn(): ScalarFunction => new Minus(ref('integer'), ref('integer'), true),
                'returns' => type_integer(),
            ],
            Mod::class => [
                'factory' => static fn(): ScalarFunction => new Mod(ref('integer'), ref('integer')),
                'returns' => type_integer(),
            ],
            ModifyDateTime::class => [
                'factory' => static fn(): ScalarFunction => new ModifyDateTime(ref('datetime'), lit('+1 day')),
                'returns' => type_datetime(),
            ],
            Multiply::class => [
                'factory' => static fn(): ScalarFunction => new Multiply(ref('integer'), ref('integer'), true),
                'returns' => type_integer(),
            ],
            Not::class => [
                'factory' => static fn(): ScalarFunction => new Not(ref('boolean')),
                'returns' => type_optional(type_boolean()),
            ],
            Now::class => [
                'factory' => static fn(): ScalarFunction => new Now(new DateTimeZone('Europe/Warsaw')),
                'returns' => type_datetime('Europe/Warsaw'),
            ],
            NumberFormat::class => [
                'factory' => static fn(): ScalarFunction => new NumberFormat(ref('float'), lit(2), ',', ' '),
                'returns' => type_string(),
            ],
            OnEach::class => [
                'factory' => static fn(): ScalarFunction => new OnEach(ref('list'), ref('element')->upper(), false),
                'returns' => type_list(type_string()),
            ],
            Optional::class => [
                'factory' => static fn(): ScalarFunction => new Optional(ref('string')->upper()),
                'returns' => type_optional(type_string()),
            ],
            Plus::class => [
                'factory' => static fn(): ScalarFunction => new Plus(ref('integer'), ref('integer'), true),
                'returns' => type_integer(),
            ],
            Power::class => [
                'factory' => static fn(): ScalarFunction => new Power(ref('integer'), ref('integer'), true),
                'returns' => type_integer(),
            ],
            Prepend::class => [
                'factory' => static fn(): ScalarFunction => new Prepend(ref('string'), lit('x')),
                'returns' => type_string(),
            ],
            RandomString::class => [
                'factory' => static fn(): ScalarFunction => new RandomString(lit(5)),
                'returns' => type_string(),
            ],
            Regex::class => [
                'factory' => static fn(): ScalarFunction => new Regex(lit('/a/'), ref('string'), offset: 1),
                'returns' => type_optional(type_array()),
            ],
            RegexAll::class => [
                'factory' => static fn(): ScalarFunction => new RegexAll(
                    lit('/a/'),
                    ref('string'),
                    PREG_PATTERN_ORDER,
                    1,
                ),
                'returns' => type_optional(type_array()),
            ],
            RegexMatch::class => [
                'factory' => static fn(): ScalarFunction => new RegexMatch(
                    lit('/a/'),
                    ref('string'),
                    PREG_OFFSET_CAPTURE,
                    1,
                ),
                'returns' => type_optional(type_boolean()),
            ],
            RegexMatchAll::class => [
                'factory' => static fn(): ScalarFunction => new RegexMatchAll(
                    lit('/a/'),
                    ref('string'),
                    PREG_SET_ORDER,
                    1,
                ),
                'returns' => type_optional(type_boolean()),
            ],
            RegexReplace::class => [
                'factory' => static fn(): ScalarFunction => new RegexReplace(lit('/a/'), lit('b'), ref('string')),
                'returns' => type_string(),
            ],
            Repeat::class => [
                'factory' => static fn(): ScalarFunction => new Repeat(ref('string'), lit(2)),
                'returns' => type_string(),
            ],
            ResolvedReference::class => [
                'factory' => static fn(): ScalarFunction => new ResolvedReference(
                    'string',
                    type_optional(type_string()),
                    'label',
                ),
                'returns' => type_optional(type_string()),
            ],
            Reverse::class => [
                'factory' => static fn(): ScalarFunction => new Reverse(ref('string')),
                'returns' => type_string(),
            ],
            Round::class => [
                'factory' => static fn(): ScalarFunction => new Round(ref('float'), 3, PHP_ROUND_HALF_EVEN),
                'returns' => type_float(),
            ],
            Sanitize::class => [
                'factory' => static fn(): ScalarFunction => new Sanitize(ref('string'), lit('*'), 2),
                'returns' => type_string(),
            ],
            Size::class => [
                'factory' => static fn(): ScalarFunction => new Size(ref('list')),
                'returns' => type_optional(type_integer()),
            ],
            Slug::class => [
                'factory' => static fn(): ScalarFunction => new Slug(ref('string'), '_', 'pl', ['&' => 'and']),
                'returns' => type_string(),
            ],
            Split::class => [
                'factory' => static fn(): ScalarFunction => new Split(ref('string'), lit(','), 3),
                'returns' => type_list(type_string()),
            ],
            Sprintf::class => [
                'factory' => static fn(): ScalarFunction => new Sprintf(lit('%s'), ref('string')),
                'returns' => type_string(),
            ],
            StartsWith::class => [
                'factory' => static fn(): ScalarFunction => new StartsWith(ref('string'), lit('a')),
                'returns' => type_optional(type_boolean()),
            ],
            StringAfter::class => [
                'factory' => static fn(): ScalarFunction => new StringAfter(ref('string'), lit('a'), true),
                'returns' => type_string(),
            ],
            StringAfterLast::class => [
                'factory' => static fn(): ScalarFunction => new StringAfterLast(ref('string'), lit('a'), true),
                'returns' => type_string(),
            ],
            StringBefore::class => [
                'factory' => static fn(): ScalarFunction => new StringBefore(ref('string'), lit('a'), true),
                'returns' => type_string(),
            ],
            StringBeforeLast::class => [
                'factory' => static fn(): ScalarFunction => new StringBeforeLast(ref('string'), lit('a'), true),
                'returns' => type_string(),
            ],
            StringContainsAny::class => [
                'factory' => static fn(): ScalarFunction => new StringContainsAny(ref('string'), lit(['a'])),
                'returns' => type_optional(type_boolean()),
            ],
            StringEqualsTo::class => [
                'factory' => static fn(): ScalarFunction => new StringEqualsTo(ref('string'), lit('x')),
                'returns' => type_optional(type_boolean()),
            ],
            StringFold::class => [
                'factory' => static fn(): ScalarFunction => new StringFold(ref('string')),
                'returns' => type_string(),
            ],
            StringMatch::class => [
                'factory' => static fn(): ScalarFunction => new StringMatch(ref('string'), lit('/a/')),
                'returns' => type_optional(type_array()),
            ],
            StringMatchAll::class => [
                'factory' => static fn(): ScalarFunction => new StringMatchAll(ref('string'), lit('/a/')),
                'returns' => type_array(),
            ],
            StringNormalize::class => [
                'factory' => static fn(): ScalarFunction => new StringNormalize(ref('string'), Normalizer::FORM_D),
                'returns' => type_string(),
            ],
            StringStyle::class => [
                'factory' => static fn(): ScalarFunction => new StringStyle(ref('string'), StringStyles::SNAKE),
                'returns' => type_string(),
            ],
            StringTitle::class => [
                'factory' => static fn(): ScalarFunction => new StringTitle(ref('string'), true),
                'returns' => type_string(),
            ],
            StringWidth::class => [
                'factory' => static fn(): ScalarFunction => new StringWidth(ref('string')),
                'returns' => type_integer(),
            ],
            StrPad::class => [
                'factory' => static fn(): ScalarFunction => new StrPad(ref('string'), lit(5), '*', STR_PAD_LEFT),
                'returns' => type_string(),
            ],
            StrReplace::class => [
                'factory' => static fn(): ScalarFunction => new StrReplace(ref('string'), lit('a'), lit('b')),
                'returns' => type_string(),
            ],
            Structure::class => [
                'factory' => static fn(): ScalarFunction => new Structure([
                    'id' => ref('string'),
                    'quantity' => ref('integer'),
                ]),
                'returns' => type_structure([
                    'id' => type_optional(type_string()),
                    'quantity' => type_optional(type_integer()),
                ]),
            ],
            StructureSelect::class => [
                'factory' => static fn(): ScalarFunction => new StructureSelect(ref('structure'), 'field'),
                'returns' => type_optional(type_structure(['field' => type_integer()])),
            ],
            ToDate::class => [
                'factory' => static fn(): ScalarFunction => new ToDate(
                    ref('string'),
                    lit('Y-m-d'),
                    new DateTimeZone('Europe/Warsaw'),
                ),
                'returns' => type_date(),
            ],
            ToDateTime::class => [
                'factory' => static fn(): ScalarFunction => new ToDateTime(
                    ref('string'),
                    lit('Y-m-d'),
                    new DateTimeZone('Europe/Warsaw'),
                ),
                'returns' => type_optional(type_datetime('Europe/Warsaw')),
            ],
            ToLower::class => [
                'factory' => static fn(): ScalarFunction => new ToLower(ref('string')),
                'returns' => type_string(),
            ],
            ToTimeZone::class => [
                'factory' => static fn(): ScalarFunction => new ToTimeZone(ref('datetime'), 'UTC'),
                'returns' => type_datetime(),
            ],
            ToUpper::class => [
                'factory' => static fn(): ScalarFunction => new ToUpper(ref('string')),
                'returns' => type_string(),
            ],
            Trim::class => [
                'factory' => static fn(): ScalarFunction => new Trim(ref('string'), TrimType::LEFT, '-'),
                'returns' => type_string(),
            ],
            Truncate::class => [
                'factory' => static fn(): ScalarFunction => new Truncate(ref('string'), lit(2), '~'),
                'returns' => type_string(),
            ],
            Ulid::class => [
                'factory' => static fn(): ScalarFunction => new Ulid('01ARZ3NDEKTSV4RRFFQ69G5FAV'),
                'returns' => type_string(),
            ],
            UnicodeLength::class => [
                'factory' => static fn(): ScalarFunction => new UnicodeLength(ref('string')),
                'returns' => type_integer(),
            ],
            UnresolvedReference::class => [
                'factory' => static fn(): ScalarFunction => new UnresolvedReference('string', 'label'),
                'returns' => null,
            ],
            Uuid::class => [
                'factory' => static fn(): ScalarFunction => Uuid::uuid7(ref('datetime')),
                'returns' => type_uuid(),
            ],
            When::class => [
                'factory' => static fn(): ScalarFunction => new When(ref('boolean'), lit(1), lit(null)),
                'returns' => type_optional(type_integer()),
            ],
            Wordwrap::class => [
                'factory' => static fn(): ScalarFunction => new Wordwrap(ref('string'), lit(5), '|', true),
                'returns' => type_string(),
            ],
            XPath::class => [
                'factory' => static fn(): ScalarFunction => new XPath(ref('xml'), lit('//a')),
                'returns' => type_optional(type_list(type_xml_element())),
            ],
        ];
    }
}
