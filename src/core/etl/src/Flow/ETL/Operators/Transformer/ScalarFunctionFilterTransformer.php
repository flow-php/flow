<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\Transformer;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\Evaluation\Filter;
use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\Operators\BoundStep;
use Flow\ETL\Operators\Transformer;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use SensitiveParameter;

final readonly class ScalarFunctionFilterTransformer implements Transformer
{
    /**
     * @param null|Filter $filter the predicate bound to the input schema; only bind() sets it
     */
    public function __construct(
        public ScalarFunction $function,
        private ?Filter $filter = null,
    ) {}

    /**
     * @throws InvalidArgumentException
     */
    public function bind(Schema $input): BoundStep
    {
        return new BoundStep(new self($this->function, Filter::bind($input, $this->function)), $input);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['function'] ?? null, ScalarFunction::class),
            ($data['filter'] ?? null) === null ? null : $rebuild->of($data['filter'], Filter::class),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, ['function' => $this->function, 'filter' => $this->filter]);
    }

    public function transform(Rows $rows, Backend $backend): Rows
    {
        return ($this->filter ?? Filter::bind($rows->schema(), $this->function))->filter($rows, $backend);
    }
}
