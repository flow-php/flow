<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Dom\HTMLElement;
use DOMDocument;
use DOMElement;
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
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_xml_element;

final class DOMElementParent implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $element;

    public function __construct(ScalarFunction|DOMNode|HTMLElement $element)
    {
        $this->element = $element instanceof ScalarFunction ? $element : new Literal($element);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self($rebuild->of($data['element'] ?? null, ScalarFunction::class));
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'element' => $this->element,
        ]);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->element];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0]);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_optional(type_xml_element());
    }

    public function eval(Rows $rows, Backend $backend): Column
    {
        $types = [
            type_instance_of(DOMNode::class),
        ];

        if (class_exists('\Dom\HTMLElement')) {
            $types[] = type_instance_of(HTMLElement::class);
        }

        $nodes = (new Parameter($this->element))->asTypes($rows, $backend, ...$types);
        $results = [];
        $i = 0;

        try {
            foreach ($nodes as $i => $node) {
                if ($node instanceof DOMDocument) {
                    $node = $node->documentElement;
                }

                if ($node === null) {
                    throw new InvalidArgumentException('DOMElementParent requires non-null DOMNode or HTMLElement.');
                }

                if ($node instanceof HTMLElement) {
                    $results[] = $node->parentElement;

                    continue;
                }

                // parentNode of a root element is the DOMDocument, which is not an xml_element.
                $results[] = $node->parentNode instanceof DOMElement ? $node->parentNode : null;
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($backend))->of($this, $results);
    }
}
