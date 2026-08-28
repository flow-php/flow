<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Dom\CharacterData;
use Dom\HTMLElement;
use DOMDocument;
use DOMElement;
use DOMNode;
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

final class DOMElementValue implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $node;

    public function __construct(ScalarFunction|DOMNode|CharacterData|HTMLElement $node)
    {
        $this->node = $node instanceof ScalarFunction ? $node : lit($node);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->node];
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
        return type_optional(type_string());
    }

    public function eval(Row $row, FlowContext $context): mixed
    {
        $types = [
            type_instance_of(DOMNode::class),
            type_list(type_instance_of(DOMNode::class)),
        ];

        if (class_exists('\Dom\HTMLElement')) {
            $types[] = type_instance_of(CharacterData::class);
            $types[] = type_instance_of(HTMLElement::class);
            $types[] = type_list(type_instance_of(HTMLElement::class));
        }

        $node = (new Parameter($this->node))->as($row, $context, ...$types);

        if (is_array($node) && count($node)) {
            $node = reset($node);
        }

        if ($node instanceof DOMDocument) {
            $node = $node->documentElement;
        }

        if ($node instanceof DOMElement) {
            return $node->nodeValue;
        }

        if ($node instanceof CharacterData || $node instanceof HTMLElement) {
            return $node->textContent;
        }

        return null;
    }
}
