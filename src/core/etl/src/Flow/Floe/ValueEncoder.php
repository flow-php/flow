<?php

declare(strict_types=1);

namespace Flow\Floe;

use DOMDocument;
use DOMElement;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Schema\Definition\BooleanDefinition;
use Flow\ETL\Schema\Definition\DateDefinition;
use Flow\ETL\Schema\Definition\DateTimeDefinition;
use Flow\ETL\Schema\Definition\EnumDefinition;
use Flow\ETL\Schema\Definition\FloatDefinition;
use Flow\ETL\Schema\Definition\HTMLDefinition;
use Flow\ETL\Schema\Definition\HTMLElementDefinition;
use Flow\ETL\Schema\Definition\IntegerDefinition;
use Flow\ETL\Schema\Definition\JsonDefinition;
use Flow\ETL\Schema\Definition\ListDefinition;
use Flow\ETL\Schema\Definition\MapDefinition;
use Flow\ETL\Schema\Definition\NullDefinition;
use Flow\ETL\Schema\Definition\StringDefinition;
use Flow\ETL\Schema\Definition\StructureDefinition;
use Flow\ETL\Schema\Definition\TimeDefinition;
use Flow\ETL\Schema\Definition\UuidDefinition;
use Flow\ETL\Schema\Definition\XMLDefinition;
use Flow\ETL\Schema\Definition\XMLElementDefinition;
use Flow\Floe\Encoding\BooleanEncoder;
use Flow\Floe\Encoding\DateTimeEncoder;
use Flow\Floe\Encoding\EnumEncoder;
use Flow\Floe\Encoding\Float64Encoder;
use Flow\Floe\Encoding\HtmlDocumentEncoder;
use Flow\Floe\Encoding\HtmlElementEncoder;
use Flow\Floe\Encoding\Int64Encoder;
use Flow\Floe\Encoding\IntervalEncoder;
use Flow\Floe\Encoding\JsonEncoder;
use Flow\Floe\Encoding\ListEncoder;
use Flow\Floe\Encoding\MapEncoder;
use Flow\Floe\Encoding\NullEncoder;
use Flow\Floe\Encoding\OptionalEncoder;
use Flow\Floe\Encoding\PackedListEncoder;
use Flow\Floe\Encoding\StringEncoder;
use Flow\Floe\Encoding\StringKeyEncoder;
use Flow\Floe\Encoding\StructureEncoder;
use Flow\Floe\Encoding\TimeZoneEncoder;
use Flow\Floe\Encoding\UuidEncoder;
use Flow\Floe\Encoding\XmlDocumentEncoder;
use Flow\Floe\Encoding\XmlElementEncoder;
use Flow\Floe\Exception\FloeException;
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

use function is_string;
use function pack;
use function sprintf;
use function strlen;

final class ValueEncoder
{
    private readonly DateTimeEncoder $dateTimeEncoder;

    private readonly JsonEncoder $jsonEncoder;

    private readonly TimeZoneEncoder $timeZoneEncoder;

    private readonly UuidEncoder $uuidEncoder;

    public function __construct()
    {
        $this->dateTimeEncoder = new DateTimeEncoder();
        $this->timeZoneEncoder = new TimeZoneEncoder();
        $this->uuidEncoder = new UuidEncoder();
        $this->jsonEncoder = new JsonEncoder();
    }

    /**
     * @throws FloeException
     */
    public function encoderFor(Definition $definition): Encoding\ValueEncoder
    {
        return match ($definition::class) {
            IntegerDefinition::class,
            FloatDefinition::class,
            BooleanDefinition::class,
            StringDefinition::class,
            DateTimeDefinition::class,
            DateDefinition::class,
            TimeDefinition::class,
            UuidDefinition::class,
            JsonDefinition::class,
            EnumDefinition::class,
            XMLDefinition::class,
            XMLElementDefinition::class,
            HTMLDefinition::class,
            HTMLElementDefinition::class,
            ListDefinition::class,
            MapDefinition::class,
            StructureDefinition::class,
            NullDefinition::class,
                => $this->elementEncoderFor($definition->type()),
            default => throw new FloeException(sprintf(
                'Floe does not support columns of type "%s"',
                $definition->type()->toString(),
            )),
        };
    }

    /**
     * Types reachable only inside containers - list elements, map keys and values,
     * structure elements - where no Definition exists.
     *
     * @param Type<mixed> $type
     *
     * @throws FloeException
     */
    private function elementEncoderFor(Type $type): Encoding\ValueEncoder
    {
        return match ($type::class) {
            IntegerType::class, PositiveIntegerType::class => new Int64Encoder(),
            FloatType::class => new Float64Encoder(),
            BooleanType::class => new BooleanEncoder(),
            StringType::class,
            NonEmptyStringType::class,
            NumericStringType::class,
            ClassStringType::class,
                => new StringEncoder(),
            TimeZoneType::class => $this->timeZoneEncoder,
            DateTimeType::class, DateType::class => $this->dateTimeEncoder,
            TimeType::class => new IntervalEncoder(),
            UuidType::class => $this->uuidEncoder,
            JsonType::class => $this->jsonEncoder,
            EnumType::class => new EnumEncoder(),
            XMLType::class => new XmlDocumentEncoder(),
            XMLElementType::class => new XmlElementEncoder(),
            HTMLType::class => new HtmlDocumentEncoder(),
            HTMLElementType::class => new HtmlElementEncoder(),
            ListType::class => $this->listEncoder($type),
            MapType::class => $this->mapEncoder($type),
            StructureType::class => $this->structureEncoder($type),
            OptionalType::class => new OptionalEncoder($this->elementEncoderFor($type->base())),
            NullType::class => new NullEncoder(),
            default => throw new FloeException(sprintf('Floe does not support values of type "%s"', $type->toString())),
        };
    }

    public static function lengthPrefixed(string $value): string
    {
        return pack('V', strlen($value)) . $value;
    }

    public static function htmlElementToString(object $value): string
    {
        /** @var \Dom\HTMLElement $value */
        // @mago-ignore analysis:non-existent-method
        // @mago-ignore analysis:mixed-assignment
        $html = $value->ownerDocument?->saveHtml($value);

        if (!is_string($html)) {
            throw new FloeException('Floe failed to convert HTMLElement to HTML string');
        }

        return $html;
    }

    public static function xmlDocumentToString(DOMDocument $value): string
    {
        $xml = $value->saveXML();

        if ($xml === false) {
            throw new FloeException('Floe failed to convert DOMDocument to XML string');
        }

        return $xml;
    }

    public static function xmlElementToString(DOMElement $value): string
    {
        $xml = $value->ownerDocument?->saveXML($value);

        if ($xml === null || $xml === false) {
            throw new FloeException('Floe failed to convert DOMElement to XML string');
        }

        return $xml;
    }

    /**
     * @param Type<mixed> $type
     */
    private function listEncoder(Type $type): Encoding\ValueEncoder
    {
        /** @var ListType<list<mixed>> $type */
        $element = $type->element();

        if ($element instanceof IntegerType) {
            return new PackedListEncoder('P');
        }

        if ($element instanceof FloatType) {
            return new PackedListEncoder('e');
        }

        return new ListEncoder($this->elementEncoderFor($element));
    }

    /**
     * @param Type<mixed> $type
     */
    private function mapEncoder(Type $type): Encoding\ValueEncoder
    {
        /** @var MapType<array<array-key, mixed>> $type */
        $key = $type->key();

        $keyEncoder = match (true) {
            $key instanceof IntegerType => new Int64Encoder(),
            $key instanceof StringType => new StringKeyEncoder(),
            default => throw new FloeException(sprintf(
                'Floe does not support map keys of type "%s"',
                $key->toString(),
            )),
        };

        return new MapEncoder($keyEncoder, $this->elementEncoderFor($type->value()));
    }

    /**
     * @param Type<mixed> $type
     */
    private function structureEncoder(Type $type): Encoding\ValueEncoder
    {
        /** @var StructureType<array<array-key, mixed>> $type */
        if ($type->allowsExtra()) {
            throw new FloeException('Floe does not support structures that allow extra values');
        }

        $elements = [];

        foreach ($type->elements() as $name => $elementType) {
            $elements[$name] = $this->elementEncoderFor($elementType);
        }

        foreach ($type->optionalElements() as $name => $elementType) {
            $elements[$name] = $this->elementEncoderFor($elementType);
        }

        return new StructureEncoder($elements);
    }
}
