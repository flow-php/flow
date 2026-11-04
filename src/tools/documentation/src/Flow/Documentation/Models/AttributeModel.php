<?php

declare(strict_types=1);

namespace Flow\Documentation\Models;

use PhpParser\ConstExprEvaluator;
use PhpParser\Node\Expr;
use PhpParser\Node\Expr\ClassConstFetch;
use PhpParser\Node\Identifier;
use PhpParser\Node\Name;
use PhpParser\PrettyPrinter\Standard;
use Roave\BetterReflection\Reflection\ReflectionAttribute;

use function array_map;
use function constant;
use function enum_exists;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_mixed;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function strrpos;
use function substr;

final readonly class AttributeModel
{
    /**
     * @param array<array-key, mixed> $arguments
     */
    public function __construct(
        public string $name,
        public string $namespace,
        public array $arguments,
    ) {}

    /**
     * @param array<string, mixed> $data
     */
    public static function fromArray(array $data): self
    {
        $data = type_structure([
            'name' => type_string(),
            'namespace' => type_string(),
            'arguments' => type_map(type_string(), type_mixed()),
        ])->assert($data);

        return new self($data['name'], $data['namespace'], $data['arguments']);
    }

    public static function fromReflection(ReflectionAttribute $reflectionAttribute): self
    {
        $separator = (int) strrpos($reflectionAttribute->getName(), '\\');
        // Better Reflection cannot compile enum cases or constants of classes it cannot load, e.g. PHP stub attributes.
        $evaluator = new ConstExprEvaluator(static fn(Expr $argument): mixed => $argument instanceof ClassConstFetch
            && $argument->class instanceof Name
            && $argument->name instanceof Identifier
            && enum_exists($argument->class->toString())
                ? constant($argument->class->toString() . '::' . $argument->name->toString())
                : (new Standard())->prettyPrintExpr($argument));

        return new self(
            substr($reflectionAttribute->getName(), $separator === 0 ? 0 : $separator + 1),
            substr($reflectionAttribute->getName(), 0, $separator),
            array_map($evaluator->evaluateDirectly(...), $reflectionAttribute->getArgumentsExpressions()),
        );
    }

    /**
     * @return array<string, mixed>
     */
    public function normalize(): array
    {
        return [
            'name' => $this->name,
            'namespace' => $this->namespace,
            'arguments' => $this->arguments,
        ];
    }
}
