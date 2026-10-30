<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\Transformer;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\ReferenceResolver;
use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\Operators\BoundStep;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use SensitiveParameter;

use function Flow\Types\DSL\type_boolean;

final class UntilTransformer implements Limiting
{
    private bool $reached = false;

    /**
     * @param null|ScalarFunction $resolved the predicate resolved against the bound schema; only bind() sets it
     */
    public function __construct(
        private readonly ScalarFunction $function,
        private ?ScalarFunction $resolved = null,
    ) {}

    public function bind(Schema $input): BoundStep
    {
        return new BoundStep(new self($this->function, $this->resolve($input)), $input);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['function'] ?? null, ScalarFunction::class),
            ($data['resolved'] ?? null) === null ? null : $rebuild->of($data['resolved'], ScalarFunction::class),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, ['function' => $this->function, 'resolved' => $this->resolved]);
    }

    public function transform(Rows $rows, Backend $backend): Rows
    {
        if ($this->reached) {
            return Rows::empty($rows->schema(), $backend);
        }

        // the unbound path has no plan to hold the resolved predicate, so it is memoised here -
        // until() stops the whole stream, not one batch
        $this->resolved ??= $this->resolve($rows->schema());

        // @mago-ignore analysis:mixed-assignment
        foreach ($this->resolved->eval($rows, $backend)->physicals() as $i => $value) {
            if (!$value) {
                $this->reached = true;

                return $rows->slice(0, $i);
            }
        }

        return $rows;
    }

    public function reached(): bool
    {
        return $this->reached;
    }

    /**
     * @throws InvalidArgumentException
     */
    private function resolve(Schema $input): ScalarFunction
    {
        return (new ReferenceResolver())->bind($this->function, $input, 'until', type_boolean());
    }
}
