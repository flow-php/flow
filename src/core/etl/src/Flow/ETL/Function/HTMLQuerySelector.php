<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Dom\Element;
use Dom\HTMLDocument;
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
use function Flow\Types\DSL\type_optional;

final class HTMLQuerySelector implements ScalarFunction
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
        return type_optional(type_html_element());
    }

    public function eval(Row $row, FlowContext $context): ?Element
    {
        $value = (new Parameter($this->value))->as(
            $row,
            $context,
            type_instance_of(HTMLDocument::class),
            type_instance_of(HTMLElement::class),
        );
        $selector = (new Parameter($this->selector))->asString($row, $context);

        if (null === $value) {
            throw new InvalidArgumentException('HTMLQuerySelector requires non-null HTMLDocument');
        }

        if (null === $selector) {
            throw new InvalidArgumentException('HTMLQuerySelector requires non-null selector');
        }

        return $value->querySelector($selector);
    }
}
