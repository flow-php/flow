<?php

declare(strict_types=1);

namespace Flow\ETL\DSL;

use Flow\ETL\DSL\Entry as DSLEntry;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row;
use Flow\ETL\Row\Entries;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\EntryFactory;
use Flow\ETL\Row\Factory\NativeEntryFactory;
use Flow\ETL\Transformer;
use Flow\ETL\Transformer\ArrayKeysStyleConverterTransformer;
use Flow\ETL\Transformer\Cast\CastJsonToArray;
use Flow\ETL\Transformer\Cast\CastToDateTime;
use Flow\ETL\Transformer\Cast\CastToInteger;
use Flow\ETL\Transformer\Cast\CastToJson;
use Flow\ETL\Transformer\Cast\CastToString;
use Flow\ETL\Transformer\Cast\EntryCaster\ArrayToListCaster;
use Flow\ETL\Transformer\Cast\EntryCaster\DateTimeToStringEntryCaster;
use Flow\ETL\Transformer\Cast\EntryCaster\StringToDateTimeEntryCaster;
use Flow\ETL\Transformer\CastTransformer;
use Flow\ETL\Transformer\Filter\Filter\EntryEqualsTo;
use Flow\ETL\Transformer\Filter\Filter\EntryExists;
use Flow\ETL\Transformer\Filter\Filter\EntryNotNull;
use Flow\ETL\Transformer\Filter\Filter\EntryNumber;
use Flow\ETL\Transformer\Filter\Filter\Opposite;
use Flow\ETL\Transformer\Filter\Filter\ValidValue;
use Flow\ETL\Transformer\FilterRowsTransformer;
use Flow\ETL\Transformer\KeepEntriesTransformer;
use Flow\ETL\Transformer\MathOperationTransformer;
use Flow\ETL\Transformer\MathValueOperationTransformer;
use Flow\ETL\Transformer\Rename\ArrayKeyRename;
use Flow\ETL\Transformer\Rename\EntryRename;
use Flow\ETL\Transformer\RenameEntriesTransformer;
use Flow\ETL\Transformer\StringEntryValueCaseConverterTransformer;
use Laminas\Hydrator\ReflectionHydrator;
use Symfony\Component\Validator\Constraint;

/**
 * @infection-ignore-all
 */
class Transform
{
    final public static function add(string $left_entry, string $right_entry, string $new_entry_name  = null) : Transformer
    {
        return MathOperationTransformer::add($left_entry, $right_entry, $new_entry_name ?? $left_entry);
    }

    /**
     * @param array<mixed> $data
     */
    final public static function add_array(string $name, array $data) : Transformer
    {
        return new Transformer\StaticEntryTransformer(DSLEntry::array($name, $data));
    }

    final public static function add_boolean(string $name, bool $value) : Transformer
    {
        return new Transformer\StaticEntryTransformer(DSLEntry::boolean($name, $value));
    }

    final public static function add_datetime(string $name, \DateTimeInterface $value) : Transformer
    {
        return new Transformer\StaticEntryTransformer(DSLEntry::datetime($name, $value));
    }

    final public static function add_datetime_from_string(string $name, string $value) : Transformer
    {
        return new Transformer\StaticEntryTransformer(DSLEntry::datetime($name, new \DateTimeImmutable($value)));
    }

    final public static function add_float(string $name, float $value) : Transformer
    {
        return new Transformer\StaticEntryTransformer(DSLEntry::float($name, $value));
    }

    final public static function add_integer(string $name, int $value) : Transformer
    {
        return new Transformer\StaticEntryTransformer(DSLEntry::integer($name, $value));
    }

    /**
     * @param array<mixed> $data
     */
    final public static function add_json(string $name, array $data) : Transformer
    {
        return new Transformer\StaticEntryTransformer(DSLEntry::json($name, $data));
    }

    final public static function add_json_from_string(string $name, string $json) : Transformer
    {
        return new Transformer\StaticEntryTransformer(DSLEntry::json($name, (array) \json_decode($json, true, 512, JSON_THROW_ON_ERROR)));
    }

    /**
     * @param array<mixed> $data
     */
    final public static function add_json_object(string $name, array $data) : Transformer
    {
        return new Transformer\StaticEntryTransformer(DSLEntry::json_object($name, $data));
    }

    final public static function add_null(string $name) : Transformer
    {
        return new Transformer\StaticEntryTransformer(DSLEntry::null($name));
    }

    final public static function add_object(string $name, object $data) : Transformer
    {
        return new Transformer\StaticEntryTransformer(DSLEntry::object($name, $data));
    }

    final public static function add_string(string $name, string $value) : Transformer
    {
        return new Transformer\StaticEntryTransformer(DSLEntry::string($name, $value));
    }

    final public static function add_value(string $left_entry, int|float $value, string $new_entry_name = null) : Transformer
    {
        return MathValueOperationTransformer::add($left_entry, $value, $new_entry_name ?? $left_entry);
    }

    /**
     * @param array<string> $keys
     */
    final public static function array_collection_get(array $keys, string $arrayEntryName, string $new_entry_name = 'element') : Transformer
    {
        return new Transformer\ArrayCollectionGetTransformer($keys, $arrayEntryName, $new_entry_name);
    }

    /**
     * @param array<string> $keys
     */
    final public static function array_collection_get_first(array $keys, string $arrayEntryName, string $new_entry_name = 'element') : Transformer
    {
        return Transformer\ArrayCollectionGetTransformer::fromFirst($keys, $arrayEntryName, $new_entry_name);
    }

    final public static function array_collection_merge(string $arrayEntryName, string $new_entry_name = 'element') : Transformer
    {
        return new Transformer\ArrayCollectionMergeTransformer($arrayEntryName, $new_entry_name);
    }

    final public static function array_convert_keys(string $array_column, string $style) : Transformer
    {
        if (!\class_exists(\Jawira\CaseConverter\Convert::class)) {
            throw new RuntimeException("Jawira\CaseConverter\Convert class not found, please require using 'composer require jawira/case-converter'");
        }

        return new ArrayKeysStyleConverterTransformer($array_column, $style);
    }

    final public static function array_expand(string $array_column, string $expanded_name = 'element') : Transformer
    {
        return new Transformer\ArrayExpandTransformer($array_column, $expanded_name);
    }

    final public static function array_get(string $array_name, string $path, string $entry_name = 'element') : Transformer
    {
        return new Transformer\ArrayDotGetTransformer($array_name, $path, $entry_name);
    }

    /**
     * @param string[] $array_names
     */
    final public static function array_merge(array $array_names, string $entry_name = 'merged') : Transformer
    {
        return new Transformer\ArrayMergeTransformer($array_names, $entry_name);
    }

    final public static function array_rename_keys(string $array_column, string $path, string $new_name) : Transformer
    {
        return new Transformer\ArrayDotRenameTransformer(new ArrayKeyRename($array_column, $path, $new_name));
    }

    final public static function array_reverse(string $array_name) : Transformer
    {
        return new Transformer\ArrayReverseTransformer($array_name);
    }

    final public static function array_sort(string $array_name, int $sort_flag = \SORT_REGULAR) : Transformer
    {
        return new Transformer\ArraySortTransformer($array_name, $sort_flag);
    }

    /**
     * @param string[] $skip_keys
     */
    final public static function array_unpack(string $array_column, string $entry_prefix = '', array $skip_keys = []) : Transformer
    {
        return new Transformer\ArrayUnpackTransformer($array_column, $skip_keys, $entry_prefix);
    }

    /**
     * @psalm-param pure-callable(Entry $entry) : Entry ...$callables
     *
     * @param callable(Entry $entry) : Entry ...$callables
     */
    final public static function callback_entry(callable ...$callables) : Transformer
    {
        return new Transformer\CallbackEntryTransformer(...$callables);
    }

    /**
     * @psalm-param pure-callable(Row) : Row $callable
     *
     * @param callable(Row) : Row $callable
     */
    final public static function callback_row(callable $callable) : Transformer
    {
        return new Transformer\CallbackRowTransformer($callable);
    }

    final public static function ceil(string $entry) : Transformer
    {
        return self::user_function([$entry], 'ceil');
    }

    final public static function chain(Transformer ...$transformers) : Transformer
    {
        return new Transformer\ChainTransformer(...$transformers);
    }

    final public static function clone_entry(string $from, string $to) : Transformer
    {
        return new Transformer\CloneEntryTransformer($from, $to);
    }

    final public static function convert_name(string $style) : Transformer
    {
        if (!\class_exists(\Jawira\CaseConverter\Convert::class)) {
            throw new RuntimeException("Jawira\CaseConverter\Convert class not found, please require using 'composer require jawira/case-converter'");
        }

        return new Transformer\EntryNameStyleConverterTransformer($style);
    }

    final public static function divide(string $left_entry, string $right_entry, string $new_entry_name = null) : Transformer
    {
        return MathOperationTransformer::divide($left_entry, $right_entry, $new_entry_name ?? $left_entry);
    }

    final public static function divide_by(string $left_entry, int|float $value, string $new_entry_name = null) : Transformer
    {
        return MathValueOperationTransformer::divide($left_entry, $value, $new_entry_name ?? $left_entry);
    }

    /**
     * @param callable(Row $row) : Entries $generator
     * @psalm-param pure-callable(Row $row) : Entries $generator
     */
    final public static function dynamic(callable $generator) : Transformer
    {
        return new Transformer\DynamicEntryTransformer($generator);
    }

    /**
     * @param mixed $value
     */
    final public static function filter_equals(string $entry, $value) : Transformer
    {
        return new FilterRowsTransformer(new EntryEqualsTo($entry, $value));
    }

    final public static function filter_exists(string $entry) : Transformer
    {
        return new FilterRowsTransformer(new EntryExists($entry));
    }

    final public static function filter_invalid(string $entry, Constraint ...$constraints) : Transformer
    {
        return new FilterRowsTransformer(new ValidValue($entry, new ValidValue\SymfonyValidator($constraints)));
    }

    /**
     * @param mixed $value
     */
    final public static function filter_not_equals(string $entry, $value) : Transformer
    {
        return new FilterRowsTransformer(new Opposite(new EntryEqualsTo($entry, $value)));
    }

    final public static function filter_not_exists(string $entry) : Transformer
    {
        return new FilterRowsTransformer(new Opposite(new EntryExists($entry)));
    }

    final public static function filter_not_null(string $entry) : Transformer
    {
        return new FilterRowsTransformer(new EntryNotNull($entry));
    }

    final public static function filter_not_number(string $entry) : Transformer
    {
        return new FilterRowsTransformer(new Opposite(new EntryNumber($entry)));
    }

    final public static function filter_null(string $entry) : Transformer
    {
        return new FilterRowsTransformer(new Opposite(new EntryNotNull($entry)));
    }

    final public static function filter_number(string $entry) : Transformer
    {
        return new FilterRowsTransformer(new EntryNumber($entry));
    }

    final public static function filter_valid(string $entry, Constraint ...$constraints) : Transformer
    {
        return new FilterRowsTransformer(new Opposite(new ValidValue($entry, new ValidValue\SymfonyValidator($constraints))));
    }

    final public static function floor(string $entry) : Transformer
    {
        return self::user_function([$entry], 'floor');
    }

    final public static function group_to_array(string $groupByEntry, string $new_entry_name) : Transformer
    {
        return new Transformer\GroupToArrayTransformer($groupByEntry, $new_entry_name);
    }

    /**
     * @param array<string> $entries
     * @param null|string $algorithm
     *
     * @throws \Flow\ETL\Exception\InvalidArgumentException
     */
    final public static function hash(array $entries, string $algorithm = null, string $new_entry_name = 'hash') : Transformer
    {
        return new Transformer\HashTransformer(
            $entries,
            $algorithm ?? (PHP_VERSION_ID >= 80100 ? 'murmur3f' : 'sha256'),
            $new_entry_name
        );
    }

    final public static function keep(string ...$entries) : Transformer
    {
        return new KeepEntriesTransformer(...$entries);
    }

    final public static function ltrim(string $entry, string $characters = " \n\r\t\v\x00") : Transformer
    {
        return self::user_function([$entry], 'ltrim', [$characters]);
    }

    final public static function modulo(string $left_entry, string $right_entry, string $new_entry_name = null) : Transformer
    {
        return MathOperationTransformer::modulo($left_entry, $right_entry, $new_entry_name ?? $left_entry);
    }

    final public static function modulo_by(string $left_entry, int|float $value, string $new_entry_name = null) : Transformer
    {
        return MathValueOperationTransformer::modulo($left_entry, $value, $new_entry_name ?? $left_entry);
    }

    final public static function multiply(string $left_entry, string $right_entry, string $new_entry_name = null) : Transformer
    {
        return MathOperationTransformer::multiply($left_entry, $right_entry, $new_entry_name ?? $left_entry);
    }

    final public static function multiply_by(string $left_entry, int|float $value, string $new_entry_name = null) : Transformer
    {
        return MathValueOperationTransformer::multiply($left_entry, $value, $new_entry_name ?? $left_entry);
    }

    /**
     * @param array<string> $entries
     *
     * @throws \Flow\ETL\Exception\InvalidArgumentException
     */
    final public static function murmur3(array $entries, string $new_entry_name = 'hash') : Transformer
    {
        return new Transformer\HashTransformer(
            $entries,
            'murmur3f',
            $new_entry_name
        );
    }

    /**
     * @param array<mixed> $parameters
     */
    final public static function object_method(string $object_name, string $method, string $entry_name = 'method_entry', array $parameters = []) : Transformer
    {
        return new Transformer\ObjectMethodTransformer($object_name, $method, $entry_name, $parameters);
    }

    final public static function power(string $left_entry, string $right_entry, string $new_entry_name = null) : Transformer
    {
        return MathOperationTransformer::power($left_entry, $right_entry, $new_entry_name ?? $left_entry);
    }

    final public static function power_of(string $left_entry, int|float $value, string $new_entry_name = null) : Transformer
    {
        return MathValueOperationTransformer::power($left_entry, $value, $new_entry_name ?? $left_entry);
    }

    final public static function prefix(string $entry, string $prefix) : Transformer
    {
        return new Transformer\StringFormatTransformer($entry, \str_replace('%', '%%', $prefix) . '%s');
    }

    final public static function remove(string ...$entries) : Transformer
    {
        return new Transformer\RemoveEntriesTransformer(...$entries);
    }

    final public static function rename(string $from, string $to) : Transformer
    {
        return new RenameEntriesTransformer(new EntryRename($from, $to));
    }

    final public static function round(string $entry, int $precision = 0, int $mode = \PHP_ROUND_HALF_UP) : Transformer
    {
        return self::user_function([$entry], 'round', [$precision, $mode]);
    }

    final public static function rtrim(string $entry, string $characters = " \n\r\t\v\x00") : Transformer
    {
        return self::user_function([$entry], 'rtrim', [$characters]);
    }

    /**
     * @param array<string> $entries
     *
     * @throws \Flow\ETL\Exception\InvalidArgumentException
     */
    final public static function sha256(array $entries, string $new_entry_name = 'hash') : Transformer
    {
        return new Transformer\HashTransformer(
            $entries,
            'sha256',
            $new_entry_name
        );
    }

    final public static function str_pad(string $entry, int $length, string $pad_string = ' ', int $type = STR_PAD_RIGHT) : Transformer
    {
        return self::user_function([$entry], 'str_pad', [$length, $pad_string, $type]);
    }

    /**
     * @param string[] $string_columns
     */
    final public static function string_concat(array $string_columns, string $glue = '', string $entry_name = 'element') : Transformer
    {
        return new Transformer\StringConcatTransformer($string_columns, $glue, $entry_name);
    }

    final public static function string_format(string $entry_name, string $format) : Transformer
    {
        return new Transformer\StringFormatTransformer($entry_name, $format);
    }

    final public static function string_lower(string ...$entry_names) : Transformer
    {
        return StringEntryValueCaseConverterTransformer::lower(...$entry_names);
    }

    final public static function string_upper(string ...$entry_names) : Transformer
    {
        return StringEntryValueCaseConverterTransformer::upper(...$entry_names);
    }

    final public static function subtract(string $left_entry, string $right_entry, string $new_entry_name = null) : Transformer
    {
        return MathOperationTransformer::subtract($left_entry, $right_entry, $new_entry_name ?? $left_entry);
    }

    final public static function subtract_value(string $left_entry, int|float $value, string $new_entry_name = null) : Transformer
    {
        return MathValueOperationTransformer::subtract($left_entry, $value, $new_entry_name ?? $left_entry);
    }

    final public static function suffix(string $entry, string $suffix) : Transformer
    {
        return new Transformer\StringFormatTransformer($entry, '%s' . \str_replace('%', '%%', $suffix));
    }

    final public static function to_array(string ...$entries) : Transformer
    {
        return new CastTransformer(Transformer\Cast\CastToArray::nullable($entries));
    }

    final public static function to_array_from_json(string ...$entries) : Transformer
    {
        return new CastTransformer(CastJsonToArray::nullable($entries));
    }

    final public static function to_array_from_object(string $entry) : Transformer
    {
        if (!\class_exists("\Laminas\Hydrator\ReflectionHydrator")) {
            throw new RuntimeException("Laminas\Hydrator\ReflectionHydrator class not found, please install it using 'composer require laminas/laminas-hydrator'");
        }

        return new Transformer\ObjectToArrayTransformer(new ReflectionHydrator(), $entry);
    }

    /**
     * @param string[] $entries
     */
    final public static function to_datetime(array $entries, ?string $timezone = null, ?string $to_timezone = null) : Transformer
    {
        return new CastTransformer(CastToDateTime::nullable($entries, $timezone, $to_timezone));
    }

    /**
     * @param array<string> $entries
     */
    final public static function to_datetime_from_string(array $entries, ?string $tz = null, ?string $to_tz = null) : Transformer
    {
        return new CastTransformer(new Transformer\Cast\CastEntries($entries, new StringToDateTimeEntryCaster($tz, $to_tz), true));
    }

    final public static function to_integer(string ...$entries) : Transformer
    {
        return new CastTransformer(CastToInteger::nullable($entries));
    }

    final public static function to_json(string ...$entries) : Transformer
    {
        return new CastTransformer(CastToJson::nullable($entries));
    }

    public static function to_list_boolean(string $entry) : Transformer
    {
        return new CastTransformer(
            new Transformer\Cast\CastEntries(
                [$entry],
                new ArrayToListCaster(Entry\TypedCollection\ScalarType::boolean),
                true
            )
        );
    }

    public static function to_list_datetime(string $entry) : Transformer
    {
        return new CastTransformer(
            new Transformer\Cast\CastEntries(
                [$entry],
                new ArrayToListCaster(Entry\TypedCollection\ObjectType::of(\DateTimeInterface::class)),
                true
            )
        );
    }

    public static function to_list_float(string $entry) : Transformer
    {
        return new CastTransformer(
            new Transformer\Cast\CastEntries(
                [$entry],
                new ArrayToListCaster(Entry\TypedCollection\ScalarType::float),
                true
            )
        );
    }

    public static function to_list_integer(string $entry) : Transformer
    {
        return new CastTransformer(
            new Transformer\Cast\CastEntries(
                [$entry],
                new ArrayToListCaster(Entry\TypedCollection\ScalarType::integer),
                true
            )
        );
    }

    public static function to_list_string(string $entry) : Transformer
    {
        return new CastTransformer(
            new Transformer\Cast\CastEntries(
                [$entry],
                new ArrayToListCaster(Entry\TypedCollection\ScalarType::string),
                true
            )
        );
    }

    final public static function to_null_from_null_string(string ...$entries) : Transformer
    {
        return new Transformer\NullStringIntoNullEntryTransformer(...$entries);
    }

    final public static function to_string(string ...$entries) : Transformer
    {
        return new CastTransformer(CastToString::nullable($entries));
    }

    /**
     * @param array<string> $entries
     */
    final public static function to_string_from_datetime(array $entries, string $format) : Transformer
    {
        return new CastTransformer(new Transformer\Cast\CastEntries($entries, new DateTimeToStringEntryCaster($format), true));
    }

    final public static function transform_if(Transformer\Condition\RowCondition $condition, Transformer $transformer) : Transformer
    {
        return new Transformer\ConditionalTransformer($condition, $transformer);
    }

    final public static function trim(string $entry, string $characters = " \n\r\t\v\x00") : Transformer
    {
        return self::user_function([$entry], 'trim', [$characters]);
    }

    /**
     * @param array<string> $entries
     * @param array<mixed> $extra_arguments
     * @param EntryFactory $entry_factory
     */
    final public static function user_function(array $entries, callable $callback, array $extra_arguments = [], EntryFactory $entry_factory = new NativeEntryFactory()) : Transformer
    {
        return new Transformer\CallUserFunctionTransformer($entries, $callback, $extra_arguments, $entry_factory);
    }
}
