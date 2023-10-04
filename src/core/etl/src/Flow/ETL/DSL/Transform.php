<?php

declare(strict_types=1);

namespace Flow\ETL\DSL;

use Flow\ETL\DSL\Entry as DSLEntry;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row;
use Flow\ETL\Row\Entries;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Factory\NativeEntryFactory;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\Schema;
use Flow\ETL\Row\ValueConverter;
use Flow\ETL\Transformer;
use Flow\ETL\Transformer\ArrayKeysStyleConverterTransformer;
use Flow\ETL\Transformer\Cast\CastJsonToArray;
use Flow\ETL\Transformer\Cast\CastToDateTime;
use Flow\ETL\Transformer\Cast\CastToInteger;
use Flow\ETL\Transformer\Cast\CastToJson;
use Flow\ETL\Transformer\Cast\CastToString;
use Flow\ETL\Transformer\Cast\EntryCaster\AnyToListCaster;
use Flow\ETL\Transformer\Cast\EntryCaster\DateTimeToStringEntryCaster;
use Flow\ETL\Transformer\Cast\EntryCaster\StringToDateTimeEntryCaster;
use Flow\ETL\Transformer\CastTransformer;
use Flow\ETL\Transformer\KeepEntriesTransformer;
use Flow\ETL\Transformer\Rename\EntryRename;
use Flow\ETL\Transformer\RenameEntriesTransformer;
use Flow\ETL\Transformer\StyleConverter\StringStyles;
use Laminas\Hydrator\ReflectionHydrator;

/**
 * @infection-ignore-all
 */
class Transform
{
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

    /**
     * @param string $array_column
     * @param string $style
     * @param ?Schema $schema Desired schema of unpacked elements. Elements not found in schema will be auto detected.
     *                        It is allowed to provide definitions only for selected elements, like for example
     *                        when converting enum string value into specific Enum.
     *
     * @throws InvalidArgumentException|RuntimeException
     */
    final public static function array_convert_keys(string $array_column, string $style, ?Schema $schema = null) : Transformer
    {
        if (!\class_exists('\Jawira\CaseConverter\Convert')) {
            throw new RuntimeException("Jawira\CaseConverter\Convert class not found, please require using 'composer require jawira/case-converter'");
        }

        return new ArrayKeysStyleConverterTransformer(
            $array_column,
            $style,
            new NativeEntryFactory($schema)
        );
    }

    /**
     * Pushes static values into existing array entry, if array entry does not exist, this transformer
     * will create one.
     *
     * @param string $array_entry
     * @param array<mixed> $values
     *
     * @return Transformer
     */
    final public static function array_push(string $array_entry, array $values = []) : Transformer
    {
        return new Transformer\ArrayPushTransformer($array_entry, $values);
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
     * @psalm-param callable(Entry $entry) : Entry ...$callables
     *
     * @param callable(Entry $entry) : Entry ...$callables
     */
    final public static function callback_entry(callable ...$callables) : Transformer
    {
        return new Transformer\CallbackEntryTransformer(...$callables);
    }

    /**
     * @param callable(Row) : Row $callable
     */
    final public static function callback_row(callable $callable) : Transformer
    {
        return new Transformer\CallbackRowTransformer($callable);
    }

    final public static function chain(Transformer ...$transformers) : Transformer
    {
        return new Transformer\ChainTransformer(...$transformers);
    }

    final public static function clone_entry(string $from, string $to) : Transformer
    {
        return new Transformer\CloneEntryTransformer($from, $to);
    }

    final public static function convert_name(string $style = StringStyles::SNAKE) : Transformer
    {
        if (!\class_exists('\Jawira\CaseConverter\Convert')) {
            throw new RuntimeException("Jawira\CaseConverter\Convert class not found, please require using 'composer require jawira/case-converter'");
        }

        return new Transformer\EntryNameStyleConverterTransformer($style);
    }

    /**
     * @param callable(Row $row) : Entries $generator
     */
    final public static function dynamic(callable $generator) : Transformer
    {
        return new Transformer\DynamicEntryTransformer($generator);
    }

    final public static function group_to_array(string $group_by_entry, string $new_entry_name) : Transformer
    {
        return new Transformer\GroupToArrayTransformer($group_by_entry, $new_entry_name);
    }

    final public static function keep(string ...$entry) : Transformer
    {
        return new KeepEntriesTransformer(...$entry);
    }

    final public static function remove(string|Reference ...$entry) : Transformer
    {
        return new Transformer\RemoveEntriesTransformer(...$entry);
    }

    final public static function rename(string $from, string $to) : Transformer
    {
        return new RenameEntriesTransformer(new EntryRename($from, $to));
    }

    public static function rename_all_case(bool $upper = false, bool $lower = false, bool $ucfirst = false, bool $ucwords = false) : Transformer
    {
        return new Transformer\RenameAllCaseTransformer($upper, $lower, $ucfirst, $ucwords);
    }

    /**
     * @param string $search
     * @param string $replace
     *
     * @return Transformer
     */
    public static function rename_str_replace_all(string $search, string $replace) : Transformer
    {
        return new Transformer\RenameStrReplaceAllEntriesTransformer($search, $replace);
    }

    final public static function to_array(string ...$entry) : Transformer
    {
        return new CastTransformer(Transformer\Cast\CastToArray::nullable($entry));
    }

    final public static function to_array_from_json(string ...$entry) : Transformer
    {
        return new CastTransformer(CastJsonToArray::nullable($entry));
    }

    final public static function to_array_from_object(string|Row\EntryReference $entry) : Transformer
    {
        if (!\class_exists("\Laminas\Hydrator\ReflectionHydrator")) {
            throw new RuntimeException("Laminas\Hydrator\ReflectionHydrator class not found, please install it using 'composer require laminas/laminas-hydrator'");
        }

        return new Transformer\ObjectToArrayTransformer(new ReflectionHydrator(), $entry);
    }

    /**
     * @param array<string>|string $entry
     */
    final public static function to_datetime(string|array $entry, ?string $timezone = null, ?string $to_timezone = null) : Transformer
    {
        return new CastTransformer(CastToDateTime::nullable(\is_string($entry) ? [$entry] : $entry, $timezone, $to_timezone));
    }

    /**
     * @param array<string>|string $entry
     */
    final public static function to_datetime_from_string(string|array $entry, ?string $tz = null, ?string $to_tz = null) : Transformer
    {
        return new CastTransformer(new Transformer\Cast\CastEntries(\is_string($entry) ? [$entry] : $entry, new StringToDateTimeEntryCaster($tz, $to_tz), true));
    }

    final public static function to_integer(string ...$entries) : Transformer
    {
        return new CastTransformer(CastToInteger::nullable($entries));
    }

    final public static function to_json(string ...$entries) : Transformer
    {
        return new CastTransformer(CastToJson::nullable($entries));
    }

    public static function to_list_of_boolean(string $entry) : Transformer
    {
        return new CastTransformer(
            new Transformer\Cast\CastEntries(
                [$entry],
                new AnyToListCaster(Entry\TypedCollection\ScalarType::boolean),
                true
            )
        );
    }

    public static function to_list_of_datetime(string $entry) : Transformer
    {
        return new CastTransformer(
            new Transformer\Cast\CastEntries(
                [$entry],
                new AnyToListCaster(Entry\TypedCollection\ObjectType::of(\DateTimeInterface::class)),
                true
            )
        );
    }

    public static function to_list_of_float(string $entry) : Transformer
    {
        return new CastTransformer(
            new Transformer\Cast\CastEntries(
                [$entry],
                new AnyToListCaster(Entry\TypedCollection\ScalarType::float),
                true
            )
        );
    }

    public static function to_list_of_integer(string $entry) : Transformer
    {
        return new CastTransformer(
            new Transformer\Cast\CastEntries(
                [$entry],
                new AnyToListCaster(Entry\TypedCollection\ScalarType::integer),
                true
            )
        );
    }

    /**
     * @param string $entry
     * @param class-string $class
     * @param null|ValueConverter $value_converter
     *
     * @return Transformer
     */
    public static function to_list_of_object(string $entry, string $class, ValueConverter $value_converter = null) : Transformer
    {
        return new CastTransformer(
            new Transformer\Cast\CastEntries(
                [$entry],
                new AnyToListCaster(Entry\TypedCollection\ObjectType::of($class), $value_converter),
                true
            )
        );
    }

    public static function to_list_of_string(string $entry) : Transformer
    {
        return new CastTransformer(
            new Transformer\Cast\CastEntries(
                [$entry],
                new AnyToListCaster(Entry\TypedCollection\ScalarType::string),
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

    /**
     * @param array<string>|string $entry
     * @param callable $callback
     * @param array<mixed> $extra_arguments
     * @param null|string $value_argument_name - when used, row value is passed to callback function under argument with given name
     *
     * @return Transformer
     */
    final public static function user_function(array|string $entry, callable $callback, array $extra_arguments = [], string $value_argument_name = null) : Transformer
    {
        return new Transformer\CallUserFunctionTransformer(\is_string($entry) ? [$entry] : $entry, $callback, $extra_arguments, $value_argument_name);
    }
}
