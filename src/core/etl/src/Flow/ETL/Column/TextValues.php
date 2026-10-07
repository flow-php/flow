<?php

declare(strict_types=1);

namespace Flow\ETL\Column;

use DateTimeInterface;
use DOMDocument;
use Flow\ETL\Column\Physical\HtmlElementPhysical;
use Flow\ETL\Column\Physical\PhysicalFor;
use Flow\ETL\Column\Physical\XmlCharacterReferences;
use Flow\ETL\Column\Physical\XmlDocumentPhysical;
use Flow\ETL\Column\Physical\XmlElementPhysical;
use Flow\ETL\Exception\RuntimeException;
use Flow\Types\Type;
use Flow\Types\Type\Logical\DateTimeType;
use Flow\Types\Type\Logical\DateType;
use Flow\Types\Type\Logical\HTMLElementType;
use Flow\Types\Type\Logical\HTMLType;
use Flow\Types\Type\Logical\JsonType;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\Logical\TimeType;
use Flow\Types\Type\Logical\UuidType;
use Flow\Types\Type\Logical\XMLElementType;
use Flow\Types\Type\Logical\XMLType;
use Flow\Types\Type\Native\BooleanType;
use Flow\Types\Type\Native\FloatType;
use Flow\Types\Type\Native\IntegerType;
use Flow\Types\Type\NonFiniteFloat;
use Flow\Types\Type\RoundTripPrecision;
use JsonException;

use function array_keys;
use function bin2hex;
use function explode;
use function Flow\Types\DSL\type_bare;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_string;
use function gmdate;
use function implode;
use function in_array;
use function intdiv;
use function json_decode;
use function json_encode;
use function sprintf;
use function str_split;
use function substr;
use function substr_replace;

use const JSON_PRESERVE_ZERO_FRACTION;
use const JSON_THROW_ON_ERROR;

final readonly class TextValues
{
    public function __construct(
        private string $dateTimeFormat = DateTimeInterface::ATOM,
        private string $dateFormat = 'Y-m-d',
    ) {}

    /**
     * @param Type<mixed> $type
     * @param list<mixed> $physicals
     *
     * @return list<mixed>
     */
    public function of(Type $type, array $physicals): array
    {
        if ($this->keepsPhysical($type)) {
            return $physicals;
        }

        $bare = type_bare($type);

        if ($bare instanceof DateTimeType) {
            /** @var list<?int> $instants */
            $instants = $physicals;

            return $this->dateTimes($bare, $instants, $this->dateTimeFormat);
        }

        if ($bare instanceof DateType) {
            /** @var list<?int> $days */
            $days = $physicals;

            return $this->dateTimes($bare, $days, $this->dateFormat);
        }

        if ($bare instanceof UuidType) {
            /** @var list<?string> $bytes */
            $bytes = $physicals;

            return $this->uuids($bytes);
        }

        if ($bare instanceof JsonType) {
            /** @var list<?string> $texts */
            $texts = $physicals;
            $values = [];

            foreach ($texts as $text) {
                $values[] = $text === null ? null : json_decode($text, false, 512, JSON_THROW_ON_ERROR);
            }

            return $values;
        }

        if ($bare instanceof HTMLElementType || $bare instanceof XMLElementType) {
            $elements = $bare instanceof HTMLElementType ? new HtmlElementPhysical() : new XmlElementPhysical();
            $values = [];

            // @mago-ignore analysis:mixed-assignment
            foreach ($physicals as $physical) {
                $values[] = $physical === null ? null : $elements->markup(type_string()->assert($physical));
            }

            return $values;
        }

        if ($bare instanceof XMLType) {
            $nodes = (new PhysicalFor())->type($bare);
            $documents = new XmlDocumentPhysical();
            $references = new XmlCharacterReferences();
            $values = [];

            // @mago-ignore analysis:mixed-assignment
            foreach ($physicals as $physical) {
                if ($physical === null) {
                    $values[] = null;

                    continue;
                }

                $text = $documents->text(type_string()->assert($physical));

                if ($text === null) {
                    $document = type_instance_of(DOMDocument::class)->assert($nodes->fromPhysical($physical));
                    $text = $document->saveXML($document->documentElement);
                }

                if ($text === false) {
                    throw new RuntimeException('Failed to serialize XML document.');
                }

                $values[] = $references->decoded($text);
            }

            return $values;
        }

        /** @var list<?array<array-key, mixed>> $containers */
        $containers = $physicals;
        $elements = new Elements();

        if ($bare instanceof ListType) {
            return $elements->shape($this->of($bare->element(), $elements->flatten($containers)), $containers);
        }

        if ($bare instanceof MapType) {
            if (!$this->keepsPhysical($bare->value())) {
                $containers = $elements->shape($this->of($bare->value(), $elements->flatten($containers)), $containers);
            }
        } elseif ($bare instanceof StructureType) {
            foreach ($bare->elements() as $element) {
                if (!$this->keepsPhysical($element->type)) {
                    $containers = $elements->withField(
                        $containers,
                        $element->name,
                        $this->of($element->type, $elements->field($containers, $element->name)),
                    );
                }
            }
        }

        $values = [];

        foreach ($containers as $container) {
            $values[] = $container === null ? null : (object) $container;
        }

        return $values;
    }

    /**
     * @param Type<mixed> $type
     */
    public function keepsPhysical(Type $type): bool
    {
        $bare = type_bare($type);

        return match (true) {
            $bare instanceof DateTimeType,
            $bare instanceof DateType,
            $bare instanceof UuidType,
            $bare instanceof JsonType,
            $bare instanceof XMLType,
            $bare instanceof XMLElementType,
            $bare instanceof HTMLElementType,
            $bare instanceof MapType,
            $bare instanceof StructureType,
                => false,
            $bare instanceof ListType => $this->keepsPhysical($bare->element()),
            default => true,
        };
    }

    /**
     * The values a database driver binds for a column: markup (xml, xml_element, html, html_element) as the text its
     * physicals render, every other type as its logical values - the driver's types convert those themselves.
     *
     * @param Type<mixed> $type
     *
     * @return list<mixed>
     */
    public function bindable(Type $type, Column $column): array
    {
        $bare = type_bare($type);

        return $bare instanceof XMLType
        || $bare instanceof XMLElementType
        || $bare instanceof HTMLType
        || $bare instanceof HTMLElementType
            ? $this->texts($type, $column->physicals())
            : $column->values();
    }

    /**
     * @param Type<mixed> $type
     * @param list<mixed> $physicals
     *
     * @return list<?string>
     */
    public function texts(Type $type, array $physicals): array
    {
        $bare = type_bare($type);

        if ($bare instanceof JsonType) {
            /** @var list<?string> */
            return $physicals;
        }

        if ($bare instanceof BooleanType) {
            $texts = [];

            // @mago-ignore analysis:mixed-assignment
            foreach ($physicals as $physical) {
                $texts[] = $physical === null ? null : ($physical === true ? 'true' : 'false');
            }

            return $texts;
        }

        if ($bare instanceof FloatType) {
            /** @var list<?float> $floats */
            $floats = $physicals;

            return $this->floats($floats);
        }

        if ($bare instanceof ListType || $bare instanceof MapType || $bare instanceof StructureType) {
            $texts = [];
            $precision = new RoundTripPrecision();
            $precision->force();

            try {
                // @mago-ignore analysis:mixed-assignment
                foreach ($this->of($type, $physicals) as $value) {
                    $texts[] = $value === null
                        ? null
                        : json_encode($value, JSON_PRESERVE_ZERO_FRACTION | JSON_THROW_ON_ERROR);
                }
            } finally {
                $precision->restore();
            }

            return $texts;
        }

        if ($bare instanceof IntegerType || $bare instanceof TimeType) {
            $texts = [];

            /** @var list<?int> $integers */
            $integers = $physicals;

            foreach ($integers as $integer) {
                $texts[] = $integer === null ? null : (string) $integer;
            }

            return $texts;
        }

        /** @var list<?string> */
        return $this->of($type, $physicals);
    }

    /**
     * @param list<?int> $physicals
     *
     * @return list<?string>
     */
    public function dateTimes(DateTimeType|DateType $type, array $physicals, string $format): array
    {
        $pattern = new DateFormatPattern($format);
        $texts = [];

        // gmdate() names the UTC zone "GMT" under T, where a DateTimeInterface in UTC names it "UTC"
        if (($type instanceof DateType || $type->zoneName() === 'UTC') && !in_array('T', $pattern->letters(), true)) {
            $segments = $pattern->segments();

            foreach ($physicals as $physical) {
                if ($physical === null) {
                    $texts[] = null;

                    continue;
                }

                if ($type instanceof DateType) {
                    $second = $physical * 86_400;
                    $micro = 0;
                } else {
                    $micro = $physical % 1_000_000;

                    if ($micro < 0) {
                        $micro += 1_000_000;
                    }

                    $second = intdiv($physical - $micro, 1_000_000);
                }

                $text = '';

                foreach ($segments as [$segment, $letter]) {
                    $text .= gmdate($segment, $second) . match ($letter) {
                        'u' => sprintf('%06d', $micro),
                        'v' => sprintf('%03d', intdiv($micro, 1000)),
                        '' => '',
                    };
                }

                $texts[] = $text;
            }

            return $texts;
        }

        $values = (new PhysicalFor())->type($type);

        foreach ($physicals as $physical) {
            $texts[] = $physical === null
                ? null
                : type_instance_of(DateTimeInterface::class)->assert($values->fromPhysical($physical))->format($format);
        }

        return $texts;
    }

    /**
     * @param list<?string> $physicals 16 raw bytes each
     *
     * @return list<?string>
     */
    public function uuids(array $physicals): array
    {
        if ($physicals === []) {
            return [];
        }

        if (!in_array(null, $physicals, true)) {
            /** @var list<string> */
            return substr_replace(
                substr_replace(
                    substr_replace(
                        substr_replace(str_split(bin2hex(implode('', $physicals)), 32), '-', 8, 0),
                        '-',
                        13,
                        0,
                    ),
                    '-',
                    18,
                    0,
                ),
                '-',
                23,
                0,
            );
        }

        $texts = [];

        foreach ($physicals as $physical) {
            if ($physical === null) {
                $texts[] = null;

                continue;
            }

            $hex = bin2hex($physical);
            $texts[] =
                substr($hex, 0, 8)
                . '-'
                . substr($hex, 8, 4)
                . '-'
                . substr($hex, 12, 4)
                . '-'
                . substr($hex, 16, 4)
                . '-'
                . substr($hex, 20);
        }

        return $texts;
    }

    /**
     * @param list<?float> $physicals
     *
     * @return list<?string>
     */
    public function floats(array $physicals): array
    {
        if ($physicals === []) {
            return [];
        }

        $precision = new RoundTripPrecision();
        $precision->force();

        try {
            try {
                $texts = explode(',', substr(
                    json_encode($physicals, JSON_PRESERVE_ZERO_FRACTION | JSON_THROW_ON_ERROR),
                    1,
                    -1,
                ));

                foreach (array_keys($texts, 'null', true) as $i) {
                    $texts[$i] = null;
                }

                return $texts;
            } catch (JsonException) {
                $texts = [];

                foreach ($physicals as $physical) {
                    $texts[] = match (true) {
                        $physical === null => null,
                        default => NonFiniteFloat::text($physical) ?? json_encode(
                            $physical,
                            JSON_PRESERVE_ZERO_FRACTION | JSON_THROW_ON_ERROR,
                        ),
                    };
                }

                return $texts;
            }
        } finally {
            $precision->restore();
        }
    }
}
