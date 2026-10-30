<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Dom\HTMlElement;
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
use function Flow\Types\DSL\type_integer;

final class DOMElementAttributesCount implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $domElement;

    public function __construct(ScalarFunction|DOMNode|HTMlElement $domElement)
    {
        $this->domElement = $domElement instanceof ScalarFunction ? $domElement : new Literal($domElement);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self($rebuild->of($data['dom_element'] ?? null, ScalarFunction::class));
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'dom_element' => $this->domElement,
        ]);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->domElement];
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
        return type_integer();
    }

    public function eval(Rows $rows, Backend $backend): Column
    {
        $types = [
            type_instance_of(DOMElement::class),
        ];

        if (class_exists('\Dom\HTMLElement')) {
            $types[] = type_instance_of(HTMLElement::class);
        }

        $domElements = (new Parameter($this->domElement))->asTypes($rows, $backend, ...$types);
        $results = [];
        $i = 0;

        try {
            foreach ($domElements as $i => $domElement) {
                if ($domElement === null) {
                    throw new InvalidArgumentException('DOMElementAttributesCount requires non-null DOMElement');
                }

                $results[] = $domElement->attributes->length ?? 0;
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($backend))->of($this, $results);
    }
}
