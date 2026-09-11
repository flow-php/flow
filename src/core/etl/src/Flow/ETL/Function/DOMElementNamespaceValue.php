<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Dom\Document;
use Dom\Element;
use Dom\HTMLElement;
use Dom\XPath;
use DOMDocument;
use DOMNode;
use DOMXPath;
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

final class DOMElementNamespaceValue implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $domElement;
    private readonly ScalarFunction $attribute;

    public function __construct(ScalarFunction|DOMNode|HTMLElement $domElement, ScalarFunction|string|null $attribute)
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

        if (class_exists('\Dom\Element')) {
            $types[] = type_instance_of(Element::class);
            $types[] = type_list(type_instance_of(Element::class));
        }

        $node = (new Parameter($this->domElement))->as($row, $context, ...$types);

        if ($node instanceof DOMDocument) {
            $node = $node->documentElement;
        }

        if (is_array($node) && count($node)) {
            $node = reset($node);
        }

        if ($node === null) {
            throw new InvalidArgumentException('DOMElementNamespaceValue requires non-null DOMNode');
        }

        if (!$node instanceof DOMNode && !$node instanceof Element) {
            return null;
        }

        $attributeName = (new Parameter($this->attribute))->asString($row, $context);

        if ($attributeName === null) {
            return $node->namespaceURI;
        }

        if (null === $node->ownerDocument) {
            return null;
        }

        $document = $node->ownerDocument;

        $xpath = $document instanceof Document ? new XPath($document) : new DOMXPath($document);

        /** @var \DOMNameSpaceNode $nsNode */
        // @mago-ignore analysis:invalid-iterator
        foreach ($xpath->query('namespace::*') ?: [] as $nsNode) {
            if ($nsNode->nodeName === $attributeName || str_starts_with($nsNode->nodeName, $attributeName . ':')) {
                return $nsNode->nodeValue;
            }
        }

        return null;
    }
}
