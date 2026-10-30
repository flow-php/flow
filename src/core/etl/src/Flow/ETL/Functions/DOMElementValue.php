<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Dom\CharacterData;
use Dom\HTMLElement;
use DOMDocument;
use DOMElement;
use DOMNode;
use Exception;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\EvaluationException;
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

final class DOMElementValue implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $node;

    public function __construct(ScalarFunction|DOMNode|CharacterData|HTMLElement $node)
    {
        $this->node = $node instanceof ScalarFunction ? $node : new Literal($node);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self($rebuild->of($data['node'] ?? null, ScalarFunction::class));
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'node' => $this->node,
        ]);
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

    public function eval(Rows $rows, Backend $backend): Column
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

        $nodes = (new Parameter($this->node))->asTypes($rows, $backend, ...$types);
        $results = [];
        $i = 0;

        try {
            foreach ($nodes as $i => $node) {
                if (is_array($node) && count($node)) {
                    $node = reset($node);
                }

                if ($node instanceof DOMDocument) {
                    $node = $node->documentElement;
                }

                if ($node instanceof DOMElement) {
                    $results[] = $node->nodeValue;

                    continue;
                }

                if ($node instanceof CharacterData || $node instanceof HTMLElement) {
                    $results[] = $node->textContent;

                    continue;
                }

                $results[] = null;
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($backend))->of($this, $results);
    }
}
