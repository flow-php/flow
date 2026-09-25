<?php

declare(strict_types=1);

namespace Flow\ETL\Exception;

use Flow\ETL\Row\Reference;
use Flow\Types\Type\Native\UnionType;

use function sprintf;

final class UnsupportedUnionTypeException extends InvalidArgumentException
{
    /**
     * @param UnionType<mixed, mixed> $type
     */
    public static function forElement(Reference $column, UnionType $type): self
    {
        return new self(sprintf(
            'Column "%s" cannot hold elements of type "%s": a column holds exactly one type, and so does every '
            . 'element inside it.'
            . "\n"
            . 'Only "null|T" is a valid union - that is a nullable element.'
            . "\n"
            . 'Possible fixes:'
            . "\n"
            . '* Declare the widest common element type, e.g. type_string()'
            . "\n"
            . '* Declare json_schema(\'%s\') when the shape is genuinely dynamic',
            $column->name(),
            $type->toString(),
            $column->name(),
        ));
    }

    /**
     * @param UnionType<mixed, mixed> $type
     */
    public static function forColumn(Reference $column, UnionType $type): self
    {
        return new self(sprintf(
            'Column "%s" cannot be typed as "%s": a column holds exactly one type.'
            . "\n"
            . 'Only "null|T" is a valid union - that is a nullable column.'
            . "\n"
            . 'Possible fixes:'
            . "\n"
            . '* Declare the widest common type: str_schema(\'%s\')'
            . "\n"
            . '* Declare json_schema(\'%s\') when the shape is genuinely dynamic',
            $column->name(),
            $type->toString(),
            $column->name(),
            $column->name(),
        ));
    }
}
