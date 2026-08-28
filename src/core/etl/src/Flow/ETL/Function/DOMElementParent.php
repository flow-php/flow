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
use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_xml_element;

final class DOMElementParent implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $element;

    public function __construct(ScalarFunction|DOMNode|HTMLElement $element)
    {
        $this->element = $element instanceof ScalarFunction ? $element : lit($element);
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

    public function eval(Row $row, FlowContext $context): DOMNode|HTMLElement|null
    {
        $types = [
            type_instance_of(DOMNode::class),
        ];

        if (class_exists('\Dom\HTMLElement')) {
            $types[] = type_instance_of(HTMLElement::class);
        }

        $node = (new Parameter($this->element))->as($row, $context, ...$types);

        if ($node instanceof DOMDocument) {
            $node = $node->documentElement;
        }

        if ($node === null) {
            throw new InvalidArgumentException('DOMElementParent requires non-null DOMNode or HTMLElement.');
        }

        if ($node instanceof HTMLElement) {
            // @mago-ignore analysis:less-specific-return-statement
            return $node->parentElement;
        }

        return $node->parentNode;
    }
}
