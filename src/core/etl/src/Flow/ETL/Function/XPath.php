<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use DOMDocument;
use DOMNameSpaceNode;
use DOMNode;
use DOMNodeList;
use DOMXPath;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_xml_element;

final class XPath implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $value;
    private readonly ScalarFunction $path;

    public function __construct(mixed $value, ScalarFunction|string $path)
    {
        $this->value = $value instanceof ScalarFunction ? $value : lit($value);
        $this->path = $path instanceof ScalarFunction ? $path : lit($path);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->value, $this->path];
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
        return type_optional(type_list(type_xml_element()));
    }

    /**
     * @return null|array<\DOMNode>
     */
    public function eval(Row $row, FlowContext $context): ?array
    {
        $value = (new Parameter($this->value))->asInstanceOf($row, $context, DOMNode::class);
        $path = (new Parameter($this->path))->asString($row, $context);

        if ($value === null) {
            throw new InvalidArgumentException('XPath requires non-null DOMNode value');
        }

        if ($path === null) {
            throw new InvalidArgumentException('XPath requires non-null path');
        }

        if (!$value instanceof DOMDocument) {
            $dom = $value->ownerDocument ?? new DOMDocument();
            $importedNode = $dom->importNode($value, true);

            if ($importedNode !== false && $importedNode->parentNode === null) {
                $dom->appendChild($importedNode);
            }

            $value = $dom;
        }

        $xpath = new DOMXPath($value);
        /** @var DOMNodeList<DOMNameSpaceNode|DOMNode>|false $result */
        $result = $xpath->query($path);

        if ($result === false || $result->length === 0) {
            return null;
        }

        $nodes = [];

        foreach ($result as $node) {
            if ($node instanceof DOMNameSpaceNode) {
                continue;
            }

            $nodes[] = $node;
        }

        return $nodes;
    }
}
