<?php

declare(strict_types=1);

namespace Flow\Floe;

use Dom\HTMLDocument;
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
use Flow\Floe\Decoding\BooleanDecoder;
use Flow\Floe\Decoding\DateTimeDecoder;
use Flow\Floe\Decoding\EnumDecoder;
use Flow\Floe\Decoding\Float64Decoder;
use Flow\Floe\Decoding\HtmlDocumentDecoder;
use Flow\Floe\Decoding\HtmlElementDecoder;
use Flow\Floe\Decoding\Int64Decoder;
use Flow\Floe\Decoding\IntervalDecoder;
use Flow\Floe\Decoding\JsonDecoder;
use Flow\Floe\Decoding\ListDecoder;
use Flow\Floe\Decoding\MapDecoder;
use Flow\Floe\Decoding\NullDecoder;
use Flow\Floe\Decoding\OptionalDecoder;
use Flow\Floe\Decoding\PackedListDecoder;
use Flow\Floe\Decoding\StringDecoder;
use Flow\Floe\Decoding\StructureDecoder;
use Flow\Floe\Decoding\TimeZoneDecoder;
use Flow\Floe\Decoding\TimeZones;
use Flow\Floe\Decoding\UuidDecoder;
use Flow\Floe\Decoding\XmlDocumentDecoder;
use Flow\Floe\Decoding\XmlElementDecoder;
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

use function class_exists;
use function sprintf;

use const LIBXML_NOERROR;

final class ValueDecoder
{
    private readonly DateTimeDecoder $dateTimeDecoder;

    private readonly JsonDecoder $jsonDecoder;

    private readonly TimeZones $timeZones;

    private readonly UuidDecoder $uuidDecoder;

    public function __construct()
    {
        $this->timeZones = new TimeZones();
        $this->dateTimeDecoder = new DateTimeDecoder($this->timeZones);
        $this->uuidDecoder = new UuidDecoder();
        $this->jsonDecoder = new JsonDecoder();
    }

    /**
     * @throws FloeException
     */
    public function decoderFor(Definition $definition): Decoding\ValueDecoder
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
                => $this->elementDecoderFor($definition->type()),
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
    private function elementDecoderFor(Type $type): Decoding\ValueDecoder
    {
        return match ($type::class) {
            IntegerType::class, PositiveIntegerType::class => new Int64Decoder(),
            FloatType::class => new Float64Decoder(),
            BooleanType::class => new BooleanDecoder(),
            StringType::class,
            NonEmptyStringType::class,
            NumericStringType::class,
            ClassStringType::class,
                => new StringDecoder(),
            TimeZoneType::class => new TimeZoneDecoder($this->timeZones),
            DateTimeType::class, DateType::class => $this->dateTimeDecoder,
            TimeType::class => new IntervalDecoder(),
            UuidType::class => $this->uuidDecoder,
            JsonType::class => $this->jsonDecoder,
            EnumType::class => new EnumDecoder(),
            XMLType::class => new XmlDocumentDecoder(),
            XMLElementType::class => new XmlElementDecoder(),
            HTMLType::class => new HtmlDocumentDecoder(),
            HTMLElementType::class => new HtmlElementDecoder(),
            ListType::class => $this->listDecoder($type),
            MapType::class => $this->mapDecoder($type),
            StructureType::class => $this->structureDecoder($type),
            OptionalType::class => $this->optionalDecoder($type),
            NullType::class => new NullDecoder(),
            default => throw new FloeException(sprintf('Floe does not support values of type "%s"', $type->toString())),
        };
    }

    public static function htmlDocumentFromString(string $html): object
    {
        if (!class_exists('\Dom\HTMLDocument')) {
            throw new FloeException('Floe cannot restore HTML values, \Dom\HTMLDocument requires PHP 8.4+');
        }

        // @mago-expect analysis:unavailable-method
        return HTMLDocument::createFromString($html, LIBXML_NOERROR);
    }

    public static function htmlElementFromString(string $html): object
    {
        if (!class_exists('\Dom\HTMLDocument')) {
            throw new FloeException('Floe cannot restore HTML values, \Dom\HTMLDocument requires PHP 8.4+');
        }

        // @mago-expect analysis:unavailable-method
        $element = HTMLDocument::createFromString(
            '<!DOCTYPE html><html><body>' . $html . '</body></html>',
            LIBXML_NOERROR,
        )->body?->firstElementChild;

        if ($element === null) {
            throw new FloeException(sprintf('Floe failed to restore HTMLElement from "%s"', $html));
        }

        return $element;
    }

    public static function xmlDocumentFromString(string $xml): DOMDocument
    {
        $document = new DOMDocument();

        if (!@$document->loadXML($xml)) {
            throw new FloeException(sprintf('Floe failed to restore DOMDocument from "%s"', $xml));
        }

        return $document;
    }

    public static function xmlElementFromString(string $xml): DOMElement
    {
        $element = self::xmlDocumentFromString($xml)->documentElement;

        if ($element === null) {
            throw new FloeException(sprintf('Floe failed to restore DOMElement from "%s"', $xml));
        }

        return $element;
    }

    /**
     * @param Type<mixed> $type
     */
    private function listDecoder(Type $type): Decoding\ValueDecoder
    {
        /** @var ListType<list<mixed>> $type */
        $element = $type->element();

        if ($element instanceof IntegerType) {
            return new PackedListDecoder('P');
        }

        if ($element instanceof FloatType) {
            return new PackedListDecoder('e');
        }

        return new ListDecoder($this->elementDecoderFor($element));
    }

    /**
     * @param Type<mixed> $type
     */
    private function mapDecoder(Type $type): Decoding\ValueDecoder
    {
        /** @var MapType<array<array-key, mixed>> $type */
        $key = $type->key();

        $keyDecoder = match (true) {
            $key instanceof IntegerType => new Int64Decoder(),
            $key instanceof StringType => new StringDecoder(),
            default => throw new FloeException(sprintf(
                'Floe does not support map keys of type "%s"',
                $key->toString(),
            )),
        };

        return new MapDecoder($keyDecoder, $this->elementDecoderFor($type->value()));
    }

    /**
     * @param Type<mixed> $type
     */
    private function optionalDecoder(Type $type): Decoding\ValueDecoder
    {
        /** @var OptionalType<mixed> $type */
        return new OptionalDecoder($this->elementDecoderFor($type->base()));
    }

    /**
     * @param Type<mixed> $type
     */
    private function structureDecoder(Type $type): Decoding\ValueDecoder
    {
        /** @var StructureType<array<array-key, mixed>> $type */
        if ($type->allowsExtra()) {
            throw new FloeException('Floe does not support structures that allow extra values');
        }

        $elements = [];

        foreach ($type->elements() as $name => $elementType) {
            $elements[$name] = $this->elementDecoderFor($elementType);
        }

        foreach ($type->optionalElements() as $name => $elementType) {
            $elements[$name] = $this->elementDecoderFor($elementType);
        }

        return new StructureDecoder($elements);
    }
}
