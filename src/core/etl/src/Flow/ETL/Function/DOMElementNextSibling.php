<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Dom\CharacterData;
use Dom\HTMLElement;
use DOMDocument;
use DOMElement;
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

final class DOMElementNextSibling implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $element;

    public function __construct(ScalarFunction|DOMNode|CharacterData|HTMLElement $element)
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

    public function eval(Row $row, FlowContext $context): ?DOMElement
    {
        $types = [
            type_instance_of(DOMNode::class),
        ];

        if (class_exists('\Dom\HTMLElement')) {
            $types[] = type_instance_of(CharacterData::class);
            $types[] = type_instance_of(HTMLElement::class);
        }

        $node = (new Parameter($this->element))->as($row, $context, ...$types);

        if ($node instanceof DOMDocument) {
            $node = $node->documentElement;
        }

        if (!$node instanceof DOMElement) {
            throw new InvalidArgumentException('DOMElementNextSibling requires DOMElement.');
        }

        // @mago-ignore analysis:impossible-condition
        if ($node instanceof CharacterData) {
            throw new InvalidArgumentException('DOMElementNextSibling requires HTMLElement.');
        }

        return $node->nextElementSibling;
    }
}
