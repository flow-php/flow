<?php

declare(strict_types=1);

namespace Flow\Floe;

use DOMDocument;
use DOMElement;
use Flow\Floe\Encoding\BooleanEncoder;
use Flow\Floe\Encoding\DateTimeEncoder;
use Flow\Floe\Encoding\DynamicEncoder;
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
use Flow\Types\Type\Logical\LiteralType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\NonEmptyStringType;
use Flow\Types\Type\Logical\NumericStringType;
use Flow\Types\Type\Logical\OptionalType;
use Flow\Types\Type\Logical\PositiveIntegerType;
use Flow\Types\Type\Logical\ScalarType;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\Logical\TimeType;
use Flow\Types\Type\Logical\TimeZoneType;
use Flow\Types\Type\Logical\UuidType;
use Flow\Types\Type\Logical\XMLElementType;
use Flow\Types\Type\Logical\XMLType;
use Flow\Types\Type\Native\ArrayType;
use Flow\Types\Type\Native\BooleanType;
use Flow\Types\Type\Native\EnumType;
use Flow\Types\Type\Native\FloatType;
use Flow\Types\Type\Native\IntegerType;
use Flow\Types\Type\Native\MixedType;
use Flow\Types\Type\Native\NullType;
use Flow\Types\Type\Native\StringType;
use Flow\Types\Type\Native\UnionType;

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
     * @param Type<mixed> $type
     *
     * @throws FloeException
     */
    public function encoderFor(Type $type): Encoding\ValueEncoder
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
            OptionalType::class => new OptionalEncoder($this->encoderFor($type->base())),
            NullType::class => new NullEncoder(),
            MixedType::class,
            UnionType::class,
            ScalarType::class,
            LiteralType::class,
            ArrayType::class,
                => $this->dynamicEncoder(),
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

    private function dynamicEncoder(): DynamicEncoder
    {
        return new DynamicEncoder($this->dateTimeEncoder);
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

        return new ListEncoder($this->encoderFor($element));
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
            default => $this->dynamicEncoder(),
        };

        return new MapEncoder($keyEncoder, $this->encoderFor($type->value()));
    }

    /**
     * @param Type<mixed> $type
     */
    private function structureEncoder(Type $type): Encoding\ValueEncoder
    {
        /** @var StructureType<array<array-key, mixed>> $type */
        $elements = [];

        foreach ($type->elements() as $name => $elementType) {
            $elements[$name] = $this->encoderFor($elementType);
        }

        foreach ($type->optionalElements() as $name => $elementType) {
            $elements[$name] = $this->encoderFor($elementType);
        }

        return new StructureEncoder($elements, $type->allowsExtra(), $this->dynamicEncoder());
    }
}
