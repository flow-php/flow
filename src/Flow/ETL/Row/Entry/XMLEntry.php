<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Schema\Definition;

/**
 * @implements Entry<\DOMDocument, array{name: string, value: string, encoding: null|string, version: null|string}>
 * @psalm-immutable
 */
final class XMLEntry implements \Stringable, Entry
{
    public function __construct(private readonly string $name, private readonly \DOMDocument $value)
    {
        if ($name === '') {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }
    }

    public static function fromString(string $name, string $value, string $version = '1.0', string $encoding = '') : self
    {
        if ($value === '') {
            throw InvalidArgumentException::because('Value cannot be empty');
        }

        $dom = new \DOMDocument($version, $encoding);
        $dom->loadXML($value);

        return new self($name, $dom);
    }

    public function __toString() : string
    {
        return $this->toString();
    }

    public function __serialize() : array
    {
        return [
            'name' => $this->name,
            'value' => $this->toString(),
            'encoding' => $this->value->encoding,
            'version' => $this->value->xmlVersion,
        ];
    }

    /**
     * @psalm-suppress MoreSpecificImplementedParamType
     * @psalm-suppress ImpureMethodCall
     * @psalm-suppress MixedArrayAccess
     * @psalm-suppress MixedArgument
     */
    public function __unserialize(array $data) : void
    {
        $dom = new \DOMDocument($data['version'] ?: '', $data['encoding'] ?: '');
        /** @psalm-suppress ArgumentTypeCoercion */
        $dom->loadXML($data['value']);

        $this->name = $data['name'];
        $this->value = $dom;
    }

    public function name() : string
    {
        return $this->name;
    }

    public function value() : \DOMDocument
    {
        return $this->value;
    }

    public function is(string $name) : bool
    {
        return $name === $this->name;
    }

    public function rename(string $name) : self
    {
        return new self($name, $this->value);
    }

    /**
     * @psalm-suppress MixedArgument
     * @psalm-suppress MoreSpecificImplementedParamType
     * @psalm-param pure-callable $mapper
     */
    public function map(callable $mapper) : self
    {
        return new self($this->name, $mapper($this->value()));
    }

    public function isEqual(Entry $entry) : bool
    {
        /**
         * @psalm-suppress ImpureMethodCall
         */
        return $this->is($entry->name()) && $entry instanceof self
            && ($this->value()->saveXML() === $entry->value()->saveXML());
    }

    public function toString() : string
    {
        /**
         * @psalm-suppress ImpureMethodCall
         */
        return (string) $this->value->saveXML();
    }

    public function definition() : Definition
    {
        return new Definition($this->name, [self::class]);
    }
}
