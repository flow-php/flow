<?php

declare(strict_types=1);

namespace Flow\ETL\DSL;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row;
use Flow\ETL\Row\Reference;
use Flow\ETL\Transformer;
use Flow\ETL\Transformer\KeepEntriesTransformer;
use Flow\ETL\Transformer\RenameEntryTransformer;
use Flow\ETL\Transformer\StyleConverter\StringStyles;

/**
 * @deprecated please use functions defined in Flow\ETL\DSL\functions.php
 *
 * @infection-ignore-all
 */
class Transform
{
    /**
     * @param callable(Row) : Row $callable
     */
    final public static function callback_row(callable $callable) : Transformer
    {
        return new Transformer\CallbackRowTransformer($callable);
    }

    final public static function convert_name(StringStyles|string $style = StringStyles::SNAKE) : Transformer
    {
        if (!\class_exists('\Jawira\CaseConverter\Convert')) {
            throw new RuntimeException("Jawira\CaseConverter\Convert class not found, please require using 'composer require jawira/case-converter'");
        }

        return new Transformer\EntryNameStyleConverterTransformer($style instanceof StringStyles ? $style : StringStyles::fromString($style));
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
