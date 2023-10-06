<?php

declare(strict_types=1);

namespace Flow\ETL\DSL;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row;
use Flow\ETL\Row\Entries;
use Flow\ETL\Row\Factory\NativeEntryFactory;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\Schema;
use Flow\ETL\Transformer;
use Flow\ETL\Transformer\ArrayKeysStyleConverterTransformer;
use Flow\ETL\Transformer\KeepEntriesTransformer;
use Flow\ETL\Transformer\RenameEntryTransformer;
use Flow\ETL\Transformer\StyleConverter\StringStyles;

/**
 * @infection-ignore-all
 */
class Transform
{
    /**
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
        return new RenameEntryTransformer($from, $to);
    }

    public static function rename_all_case(bool $upper = false, bool $lower = false, bool $ucfirst = false, bool $ucwords = false) : Transformer
    {
        return new Transformer\RenameAllCaseTransformer($upper, $lower, $ucfirst, $ucwords);
    }

    public static function rename_str_replace_all(string $search, string $replace) : Transformer
    {
        return new Transformer\RenameStrReplaceAllEntriesTransformer($search, $replace);
    }
}
