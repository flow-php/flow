<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Dom\HTMLDocument;
use Dom\HTMLElement;
use Exception;
use Flow\ETL\Column\Column;
use Flow\ETL\Exception\EvaluationException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RequiredPHPVersionException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Evaluation\ResultColumn;
use Flow\ETL\Rows;
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

    public function eval(Rows $rows, FlowContext $context): Column
    {
        $values = (new Parameter($this->value))->asTypes(
            $rows,
            $context,
            type_instance_of(HTMLDocument::class),
            type_instance_of(HTMLElement::class),
        );
        $selectors = (new Parameter($this->selector))->asStrings($rows, $context);
        $results = [];
        $i = 0;

        try {
            foreach ($values as $i => $value) {
                $selector = $selectors[$i];

                if (null === $value) {
                    throw new InvalidArgumentException('HTMLQuerySelector requires non-null HTMLDocument');
                }

                if (null === $selector) {
                    throw new InvalidArgumentException('HTMLQuerySelector requires non-null selector');
                }

                $results[] = $value->querySelector($selector);
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}
