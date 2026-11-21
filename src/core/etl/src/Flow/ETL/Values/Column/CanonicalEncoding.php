<?php

declare(strict_types=1);

namespace Flow\ETL\Values\Column;

use Closure;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Values\Column\Physical\HtmlElementPhysical;
use Flow\ETL\Values\Column\Physical\XmlElementPhysical;
use Flow\Types\Type;
use Flow\Types\Type\Logical\ClassStringType;
use Flow\Types\Type\Logical\DateTimeType;
use Flow\Types\Type\Logical\DateType;
use Flow\Types\Type\Logical\HTMLElementType;
use Flow\Types\Type\Logical\HTMLType;
use Flow\Types\Type\Logical\JsonType;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\NonEmptyStringType;
use Flow\Types\Type\Logical\NumericStringType;
use Flow\Types\Type\Logical\OptionalType;
use Flow\Types\Type\Logical\PositiveIntegerType;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\Logical\TimeType;
use Flow\Types\Type\Logical\TimeZoneType;
use Flow\Types\Type\Logical\UuidType;
use Flow\Types\Type\Logical\XMLElementType;
use Flow\Types\Type\Logical\XMLType;
use Flow\Types\Type\Native\BooleanType;
use Flow\Types\Type\Native\EnumType;
use Flow\Types\Type\Native\FloatType;
use Flow\Types\Type\Native\IntegerType;
use Flow\Types\Type\Native\NullType;
use Flow\Types\Type\Native\StringType;

use function array_fill;
use function array_key_exists;
use function array_map;
use function count;
use function floatval;
use function implode;
use function in_array;
use function intval;
use function is_float;
use function is_nan;
use function is_string;
use function ksort;
use function max;
use function pack;
use function sort;
use function sprintf;
use function str_split;
use function strlen;

use const SORT_STRING;

final readonly class CanonicalEncoding
{
    public const string NULL = "\x00";

    private const string BOOLEAN = "\x01";

    private const string INTEGER = "\x02";

    private const string FLOAT = "\x03";

    private const string STRING = "\x04";

    private const string JSON = "\x05";

    private const string ENUM = "\x06";

    private const string XML = "\x07";

    private const string HTML = "\x08";

    private const string TIMEZONE = "\x09";

    private const string UUID = "\x0A";

    private const string DATE = "\x0B";

    private const string DATETIME = "\x0C";

    private const string TIME = "\x0D";

    private const string LIST = "\x0E";

    private const string MAP = "\x0F";

    private const string STRUCTURE = "\x10";

    private const string XML_ELEMENT = "\x11";

    private const string HTML_ELEMENT = "\x12";

    private const string NAN = "\x00\x00\x00\x00\x00\x00\xF8\x7F";

    private const string ZERO = "\x00\x00\x00\x00\x00\x00\x00\x00";

    /**
     * @param list<Column> $columns
     *
     * @throws InvalidArgumentException
     *
     * @return list<string>
     */
    public function ordered(array $columns, int $count): array
    {
        $encoded = array_fill(0, max(0, $count), '');

        foreach ($columns as $column) {
            $this->append($encoded, $column, '', self::NULL);
        }

        return $encoded;
    }

    /**
     * @param array<array-key, Column> $columns
     *
     * @throws InvalidArgumentException
     *
     * @return list<string>
     */
    public function named(array $columns, int $count): array
    {
        $encoded = array_fill(0, max(0, $count), '');
        ksort($columns, SORT_STRING);

        foreach ($columns as $name => $column) {
            $this->append($encoded, $column, pack('P', strlen((string) $name)) . $name, '');
        }

        return $encoded;
    }

    /**
     * @param list<string> $encoded
     * @param string $part written before each non-null value
     * @param string $null written for each null value
     *
     * @throws InvalidArgumentException
     */
    private function append(array &$encoded, Column $column, string $part, string $null): void
    {
        $type = $column->type();
        $base = $type instanceof OptionalType ? $type->base() : $type;
        $physicals = $column->physicals();

        if ($base instanceof IntegerType || $base instanceof PositiveIntegerType) {
            $this->fixedWidth($encoded, $physicals, $part . self::INTEGER, $null, 'P', 8);
        } elseif ($base instanceof DateTimeType) {
            $this->fixedWidth($encoded, $physicals, $part . self::DATETIME, $null, 'P', 8);
        } elseif ($base instanceof TimeType) {
            $this->fixedWidth($encoded, $physicals, $part . self::TIME, $null, 'P', 8);
        } elseif ($base instanceof DateType) {
            $this->fixedWidth($encoded, $physicals, $part . self::DATE, $null, 'V', 4);
        } elseif ($base instanceof FloatType) {
            $this->floats($encoded, $physicals, $part . self::FLOAT, $null);
        } elseif ($base instanceof BooleanType) {
            $this->booleans($encoded, $physicals, $part, $null);
        } elseif (
            $base instanceof StringType
            || $base instanceof NonEmptyStringType
            || $base instanceof NumericStringType
            || $base instanceof ClassStringType
        ) {
            $this->texts($encoded, $physicals, $part . self::STRING, $null);
        } elseif ($base instanceof JsonType) {
            $this->texts($encoded, $physicals, $part . self::JSON, $null);
        } elseif ($base instanceof EnumType) {
            $this->texts($encoded, $physicals, $part . self::ENUM, $null);
        } elseif ($base instanceof XMLType) {
            $this->texts($encoded, $physicals, $part . self::XML, $null);
        } elseif ($base instanceof HTMLType) {
            $this->texts($encoded, $physicals, $part . self::HTML, $null);
        } elseif ($base instanceof TimeZoneType) {
            $this->texts($encoded, $physicals, $part . self::TIMEZONE, $null);
        } else {
            $write = $this->writer($type);

            // @mago-ignore analysis:mixed-assignment
            foreach ($physicals as $i => $physical) {
                $encoded[$i] .= $physical === null ? $null : $part . $write($physical);
            }
        }
    }

    /**
     * @param list<string> $encoded
     * @param list<mixed> $physicals ints or nulls
     * @param positive-int $width
     */
    private function fixedWidth(
        array &$encoded,
        array $physicals,
        string $prefix,
        string $null,
        string $format,
        int $width,
    ): void {
        // one pack() per column: a pack() per value costs more than splitting the packed string
        $packed = str_split(
            pack($format . '*', ...in_array(null, $physicals, true) ? array_map(intval(...), $physicals) : $physicals),
            $width,
        );

        // @mago-ignore analysis:mixed-assignment
        foreach ($physicals as $i => $physical) {
            $encoded[$i] .= $physical === null ? $null : $prefix . $packed[$i];
        }
    }

    /**
     * @param list<string> $encoded
     * @param list<mixed> $physicals floats or nulls
     */
    private function floats(array &$encoded, array $physicals, string $prefix, string $null): void
    {
        $packed = str_split(
            pack('e*', ...in_array(null, $physicals, true) ? array_map(floatval(...), $physicals) : $physicals),
            8,
        );

        // @mago-ignore analysis:mixed-assignment
        foreach ($physicals as $i => $physical) {
            $encoded[$i] .= match (true) {
                $physical === null => $null,
                $physical === 0.0 => $prefix . self::ZERO,
                is_float($physical) && is_nan($physical) => $prefix . self::NAN,
                default => $prefix . $packed[$i],
            };
        }
    }

    /**
     * @param list<string> $encoded
     * @param list<mixed> $physicals booleans or nulls
     */
    private function booleans(array &$encoded, array $physicals, string $part, string $null): void
    {
        $true = $part . self::BOOLEAN . "\x01";
        $false = $part . self::BOOLEAN . "\x00";

        // @mago-ignore analysis:mixed-assignment
        foreach ($physicals as $i => $physical) {
            $encoded[$i] .= match ($physical) {
                null => $null,
                true => $true,
                default => $false,
            };
        }
    }

    /**
     * @param list<string> $encoded
     * @param list<mixed> $physicals strings or nulls
     */
    private function texts(array &$encoded, array $physicals, string $prefix, string $null): void
    {
        $lengths = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($physicals as $i => $physical) {
            $encoded[$i] .= is_string($physical)
                ? $prefix . ($lengths[strlen($physical)] ??= pack('P', strlen($physical))) . $physical
                : $null;
        }
    }

    /**
     * @param Type<mixed> $type
     *
     * @throws InvalidArgumentException
     *
     * @return Closure(mixed): string takes a non-null physical of $type
     */
    private function writer(Type $type): Closure
    {
        $base = $type instanceof OptionalType ? $type->base() : $type;

        return match (true) {
            $base instanceof BooleanType => static fn(bool $physical): string => $physical
                ? self::BOOLEAN . "\x01"
                : self::BOOLEAN . "\x00",
            $base instanceof IntegerType,
            $base instanceof PositiveIntegerType,
                => static fn(int $physical): string => self::INTEGER . pack('P', $physical),
            $base instanceof FloatType => static fn(float $physical): string => (
                self::FLOAT . (is_nan($physical) ? self::NAN : pack('e', $physical === 0.0 ? 0.0 : $physical))
            ),
            $base instanceof StringType,
            $base instanceof NonEmptyStringType,
            $base instanceof NumericStringType,
            $base instanceof ClassStringType,
                => $this->text(self::STRING),
            $base instanceof JsonType => $this->text(self::JSON),
            $base instanceof EnumType => $this->text(self::ENUM),
            $base instanceof XMLType => $this->text(self::XML),
            $base instanceof HTMLType => $this->text(self::HTML),
            $base instanceof XMLElementType => $this->element(
                self::XML_ELEMENT,
                (new XmlElementPhysical())->markup(...),
            ),
            $base instanceof HTMLElementType => $this->element(
                self::HTML_ELEMENT,
                (new HtmlElementPhysical())->markup(...),
            ),
            $base instanceof TimeZoneType => $this->text(self::TIMEZONE),
            $base instanceof UuidType => static fn(string $physical): string => self::UUID . $physical,
            $base instanceof DateType => static fn(int $physical): string => self::DATE . pack('V', $physical),
            $base instanceof DateTimeType => static fn(int $physical): string => self::DATETIME . pack('P', $physical),
            $base instanceof TimeType => static fn(int $physical): string => self::TIME . pack('P', $physical),
            $base instanceof ListType => $this->list($this->writer($base->element())),
            $base instanceof MapType => $this->map(
                $this->writer($base->key()),
                $this->writer($base->value()),
                $base->key() instanceof IntegerType,
            ),
            $base instanceof StructureType => $this->structure($base),
            $base instanceof NullType => static fn(mixed $physical): string => self::NULL,
            default => throw new InvalidArgumentException(sprintf(
                'type %s has no canonical encoding, declare the type of the column',
                $base->toString(),
            )),
        };
    }

    /**
     * @return Closure(string): string
     */
    private function text(string $tag): Closure
    {
        return static fn(string $physical): string => $tag . pack('P', strlen($physical)) . $physical;
    }

    /**
     * @param Closure(string): string $markup
     *
     * @return Closure(string): string
     */
    private function element(string $tag, Closure $markup): Closure
    {
        return static function (string $physical) use ($tag, $markup): string {
            $text = $markup($physical);

            return $tag . pack('P', strlen($text)) . $text;
        };
    }

    /**
     * @param Closure(mixed): string $element
     *
     * @return Closure(list<mixed>): string
     */
    private function list(Closure $element): Closure
    {
        return static function (array $physical) use ($element): string {
            $encoded = self::LIST . pack('P', count($physical));

            // @mago-ignore analysis:mixed-assignment
            foreach ($physical as $value) {
                $encoded .= $value === null ? self::NULL : $element($value);
            }

            return $encoded;
        };
    }

    /**
     * @param Closure(mixed): string $key
     * @param Closure(mixed): string $value
     *
     * @return Closure(array<array-key, mixed>): string
     */
    private function map(Closure $key, Closure $value, bool $integerKeys): Closure
    {
        return static function (array $physical) use ($key, $value, $integerKeys): string {
            $entries = [];

            // @mago-ignore analysis:mixed-assignment
            foreach ($physical as $name => $element) {
                $entries[] =
                    $key($integerKeys ? $name : (string) $name) . ($element === null ? self::NULL : $value($element));
            }

            sort($entries, SORT_STRING);

            return self::MAP . pack('P', count($entries)) . implode('', $entries);
        };
    }

    /**
     * @param StructureType<array<array-key, mixed>> $type
     *
     * @throws InvalidArgumentException
     *
     * @return Closure(array<array-key, mixed>): string
     */
    private function structure(StructureType $type): Closure
    {
        $elements = [];
        $parts = [];

        foreach ($type->elements() as $element) {
            $name = (string) $element->name;
            $elements[$name] = $this->writer($element->type);
            $parts[$name] = pack('P', strlen($name)) . $name;
        }

        ksort($elements, SORT_STRING);

        return static function (array $physical) use ($elements, $parts): string {
            $encoded = '';
            $present = 0;

            foreach ($elements as $name => $element) {
                if (array_key_exists($name, $physical) && $physical[$name] !== null) {
                    $encoded .= $parts[$name] . $element($physical[$name]);
                    $present++;
                }
            }

            return self::STRUCTURE . pack('P', $present) . $encoded;
        };
    }
}
