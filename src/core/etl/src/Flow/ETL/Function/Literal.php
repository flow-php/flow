<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Closure;
use Flow\ETL\Column\Backend;
use Flow\ETL\Column\Column;
use Flow\ETL\Column\ValueColumn;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Evaluation\ResultColumn;
use Flow\ETL\Rows;
use Flow\Types\Type;
use Flow\Types\Type\Native\NullType;
use Flow\Types\Type\TypeDetector;

use function array_fill;
use function array_walk_recursive;
use function Flow\ETL\DSL\definition_from_type;
use function Flow\Types\DSL\type_bare;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_optional;
use function is_array;

final class Literal implements ScalarFunction
{
    use ScalarFunctionChain;

    /**
     * The column of the last evaluated batch, handed out again for a batch of the same size.
     */
    private ?Column $column = null;

    /**
     * The backend $column was built in: a column is reused only for the same row count in the same backend.
     */
    private ?Backend $columnBackend = null;

    public function __construct(
        private readonly mixed $value,
    ) {
        if ($this->value instanceof Closure) {
            throw new InvalidArgumentException(
                'A Closure cannot be used as a literal value: pipeline objects must not hold executable state.',
            );
        }

        if (is_array($this->value)) {
            $value = $this->value;

            array_walk_recursive($value, static function (mixed $leaf): void {
                if ($leaf instanceof Closure) {
                    throw new InvalidArgumentException(
                        'A Closure cannot be used as a literal value: pipeline objects must not hold executable state.',
                    );
                }
            });
        }
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return $this;
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        // lit(null) is a nullable null column - a bare NullType would count as NOT NULL.
        return $this->value === null ? type_optional(type_null()) : (new TypeDetector())->detectType($this->value);
    }

    public function eval(Rows $rows, FlowContext $context): Column
    {
        if ($this->column?->count() === $rows->count() && $this->columnBackend === $context->backend()) {
            return $this->column;
        }

        $this->columnBackend = $context->backend();
        $type = type_bare($this->returns());
        $type = $type instanceof NullType ? $type : type_optional($type);

        // @mago-ignore analysis:possibly-invalid-argument
        return $this->column = (new ResultColumn($context->backend()))->holdsLosslessly($type)
            ? $context->backend()->constant(definition_from_type('value', $type), $this->value, $rows->count())
            : new ValueColumn($rows->isEmpty() ? [] : array_fill(0, $rows->count(), $this->value));
    }

    public function value(): mixed
    {
        return $this->value;
    }
}
