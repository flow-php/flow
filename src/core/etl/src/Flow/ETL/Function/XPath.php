<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use DOMDocument;
use DOMElement;
use DOMNameSpaceNode;
use DOMNode;
use DOMNodeList;
use DOMXPath;
use Exception;
use Flow\ETL\Column\Column;
use Flow\ETL\Exception\EvaluationException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Evaluation\ResultColumn;
use Flow\ETL\Rows;
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

    public function eval(Rows $rows, FlowContext $context): Column
    {
        $values = (new Parameter($this->value))->asInstancesOf($rows, $context, DOMNode::class);
        $paths = (new Parameter($this->path))->asStrings($rows, $context);
        $results = [];
        $i = 0;

        try {
            foreach ($values as $i => $value) {
                $path = $paths[$i];

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
                    $results[] = null;

                    continue;
                }

                $nodes = [];

                foreach ($result as $node) {
                    // text(), attribute and comment queries yield nodes the declared list<xml_element> cannot hold.
                    if (!$node instanceof DOMElement) {
                        continue;
                    }

                    $nodes[] = $node;
                }

                $results[] = $nodes === [] ? null : $nodes;
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}
