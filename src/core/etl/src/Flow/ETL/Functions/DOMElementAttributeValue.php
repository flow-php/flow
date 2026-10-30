<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Dom\HTMLElement;
use DOMDocument;
use DOMNode;
use Exception;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\EvaluationException;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\Evaluation\ResultColumn;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;
use SensitiveParameter;

use function class_exists;
use function count;
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
        $this->domElement = $domElement instanceof ScalarFunction ? $domElement : new Literal($domElement);
        $this->attribute = $attribute instanceof ScalarFunction ? $attribute : new Literal($attribute);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['dom_element'] ?? null, ScalarFunction::class),
            $rebuild->of($data['attribute'] ?? null, ScalarFunction::class),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'dom_element' => $this->domElement,
            'attribute' => $this->attribute,
        ]);
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

    public function eval(Rows $rows, Backend $backend): Column
    {
        $types = [
            type_instance_of(DOMNode::class),
            type_list(type_instance_of(DOMNode::class)),
        ];

        if (class_exists('\Dom\HTMLElement')) {
            $types[] = type_instance_of(HTMLElement::class);
            $types[] = type_list(type_instance_of(HTMLElement::class));
        }

        $nodes = (new Parameter($this->domElement))->asTypes($rows, $backend, ...$types);
        $attributeNames = (new Parameter($this->attribute))->asStrings($rows, $backend);
        $results = [];
        $i = 0;

        try {
            foreach ($nodes as $i => $node) {
                $attributeName = $attributeNames[$i];

                if ($node instanceof DOMDocument) {
                    $node = $node->documentElement;
                }

                if (is_array($node) && count($node)) {
                    $node = reset($node);
                }

                if ($node === null) {
                    throw new InvalidArgumentException('DOMElementAttributeValue requires non-null DOMNode');
                }

                if ($attributeName === null) {
                    throw new InvalidArgumentException('DOMElementAttributeValue requires non-null attribute name');
                }

                if (!$node instanceof DOMNode && !$node instanceof HTMLElement) {
                    $results[] = null;

                    continue;
                }

                $attributes = $node->attributes;

                if ($attributes === null) {
                    $results[] = null;

                    continue;
                }

                $namedItem = $attributes->getNamedItem($attributeName);

                if ($namedItem === null) {
                    $results[] = null;

                    continue;
                }

                $results[] = $namedItem->nodeValue;
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($backend))->of($this, $results);
    }
}
