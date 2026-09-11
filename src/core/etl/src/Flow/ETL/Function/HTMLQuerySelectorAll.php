<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use DOM\Element;
use DOM\HTMLDocument;
use Dom\HTMLElement;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RequiredPHPVersionException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;

use function class_exists;
use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_html_element;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_optional;

final class HTMLQuerySelectorAll implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $value;
    private readonly ScalarFunction $selector;

    public function __construct(mixed $value, ScalarFunction|string $selector)
    {
        if (!class_exists('\Dom\HTMLDocument')) {
            throw new RequiredPHPVersionException('\Dom\HTMLDocument', '8.4');
        }

        $this->value = $value instanceof ScalarFunction ? $value : lit($value);
        $this->selector = $selector instanceof ScalarFunction ? $selector : lit($selector);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->value, $this->selector];
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
        return type_optional(type_list(type_html_element()));
    }

    /**
     * @return null|array<Element>
     */
    public function eval(Row $row, FlowContext $context): ?array
    {
        $value = (new Parameter($this->value))->as(
            $row,
            $context,
            type_instance_of(HTMLDocument::class),
            type_instance_of(HTMLElement::class),
        );
        $selector = (new Parameter($this->selector))->asString($row, $context);

        if (null === $value) {
            throw new InvalidArgumentException('HTMLQuerySelectorAll requires non-null HTMLDocument');
        }

        if (null === $selector) {
            throw new InvalidArgumentException('HTMLQuerySelectorAll requires non-null selector');
        }

        $result = $value->querySelectorAll($selector);

        if (0 === $result->count()) {
            return null;
        }

        $nodes = [];

        foreach ($result as $node) {
            if (!$node instanceof Element) {
                continue;
            }

            $nodes[] = $node;
        }

        return $nodes;
    }
}
