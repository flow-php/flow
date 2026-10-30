<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use DOM\Element;
use DOM\HTMLDocument;
use Dom\HTMLElement;
use Exception;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\EvaluationException;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\RequiredPHPVersionException;
use Flow\ETL\Functions\Evaluation\ResultColumn;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;
use SensitiveParameter;

use function class_exists;
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

        $this->value = $value instanceof ScalarFunction ? $value : new Literal($value);
        $this->selector = $selector instanceof ScalarFunction ? $selector : new Literal($selector);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['value'] ?? null, ScalarFunction::class),
            $rebuild->of($data['selector'] ?? null, ScalarFunction::class),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'value' => $this->value,
            'selector' => $this->selector,
        ]);
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

    public function eval(Rows $rows, Backend $backend): Column
    {
        $values = (new Parameter($this->value))->asTypes(
            $rows,
            $backend,
            type_instance_of(HTMLDocument::class),
            type_instance_of(HTMLElement::class),
        );
        $selectors = (new Parameter($this->selector))->asStrings($rows, $backend);
        $results = [];
        $i = 0;

        try {
            foreach ($values as $i => $value) {
                $selector = $selectors[$i];

                if (null === $value) {
                    throw new InvalidArgumentException('HTMLQuerySelectorAll requires non-null HTMLDocument');
                }

                if (null === $selector) {
                    throw new InvalidArgumentException('HTMLQuerySelectorAll requires non-null selector');
                }

                $result = $value->querySelectorAll($selector);

                if (0 === $result->count()) {
                    $results[] = null;

                    continue;
                }

                $nodes = [];

                foreach ($result as $node) {
                    if (!$node instanceof Element) {
                        continue;
                    }

                    $nodes[] = $node;
                }

                $results[] = $nodes;
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($backend))->of($this, $results);
    }
}
