<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Dom\HTMLElement;
use DOMDocument;
use DOMNode;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;

use function class_exists;
use function count;
use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function is_array;
use function reset;

final class DOMElementAttributeValue implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $domElement;
    private readonly ScalarFunction $attribute;

    public function __construct(ScalarFunction|DOMNode|HTMLElement $domElement, ScalarFunction|string $attribute)
    {
        $this->domElement = $domElement instanceof ScalarFunction ? $domElement : lit($domElement);
        $this->attribute = $attribute instanceof ScalarFunction ? $attribute : lit($attribute);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->domElement, $this->attribute];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0], $children[1]);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_optional(type_string());
    }

    public function eval(Row $row, FlowContext $context): ?string
    {
        $types = [
            type_instance_of(DOMNode::class),
            type_list(type_instance_of(DOMNode::class)),
        ];

        if (class_exists('\Dom\HTMLElement')) {
            $types[] = type_instance_of(HTMLElement::class);
            $types[] = type_list(type_instance_of(HTMLElement::class));
        }

        $node = (new Parameter($this->domElement))->as($row, $context, ...$types);

        if ($node instanceof DOMDocument) {
            $node = $node->documentElement;
        }

        if (is_array($node) && count($node)) {
            $node = reset($node);
        }

        $attributeName = (new Parameter($this->attribute))->asString($row, $context);

        if ($node === null) {
            throw new InvalidArgumentException('DOMElementAttributeValue requires non-null DOMNode');
        }

        if ($attributeName === null) {
            throw new InvalidArgumentException('DOMElementAttributeValue requires non-null attribute name');
        }

        if (!$node instanceof DOMNode && !$node instanceof HTMLElement) {
            return null;
        }

        $attributes = $node->attributes;

        if ($attributes === null) {
            return null;
        }

        $namedItem = $attributes->getNamedItem($attributeName);

        if ($namedItem === null) {
            return null;
        }

        return $namedItem->nodeValue;
    }
}
