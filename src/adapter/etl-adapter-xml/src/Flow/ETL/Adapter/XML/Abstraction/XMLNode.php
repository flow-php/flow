<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Abstraction;

use Flow\ETL\Exception\InvalidArgumentException;

final class XMLNode
{
    /**
     * @param string $name
     * @param array<XMLAttribute> $attributes
     * @param array<XMLNode> $children
     *
     * @throws InvalidArgumentException
     */
    private function __construct(
        public readonly string $name,
        public readonly ?string $value,
        public readonly XMLNodeType $type,
        public readonly array $attributes = [],
        public readonly array $children = []
    ) {
        if (!\mb_strlen($name)) {
            throw new InvalidArgumentException('XMLNode name can not be empty');
        }
    }

    public static function flatNode(string $name, ?string $value) : self
    {
        return new self($name, $value, XMLNodeType::FLAT);
    }

    public static function nestedNode(string $name) : self
    {
        return new self($name, null, XMLNodeType::NESTED);
    }

    public function append(self|XMLAttribute $element) : self
    {
        if ($element instanceof XMLAttribute) {
            return $this->appendAttribute($element);
        }

        return $this->appendChild($element);
    }

    public function appendAttribute(XMLAttribute $attribute) : self
    {
        return new self(
            $this->name,
            $this->value,
            $this->type,
            [...$this->attributes, $attribute],
            $this->children
        );
    }

    public function appendChild(self $child) : self
    {
        if ($this->type === XMLNodeType::FLAT) {
            throw new InvalidArgumentException('XMLNode can not have children if it has value');
        }

        return new self(
            $this->name,
            $this->value,
            $this->type,
            $this->attributes,
            [...$this->children, $child]
        );
    }

    public function hasChildren() : bool
    {
        return \count($this->children) > 0;
    }

    /**
     * @psalm-assert-if-true !null $this->value
     */
    public function hasValue() : bool
    {
        return $this->value !== null;
    }
}
