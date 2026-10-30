<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\Processor;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\Functions\Reference\References;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\Kernels\Bucketing\KeyEncoder;
use Flow\ETL\Kernels\Bucketing\SeenKeys;
use Flow\ETL\Operators\BoundStep;
use Flow\ETL\Operators\Processor;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Generator;
use SensitiveParameter;

use function array_map;
use function array_values;
use function Flow\Types\DSL\type_array;
use function is_string;

final readonly class DistinctProcessor implements Distributable
{
    /**
     * @param list<Reference|string> $columns
     * @param null|Schema $input the bound input, null unbound
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        public array $columns,
        public ?Schema $input = null,
    ) {
        if ($columns === []) {
            throw new InvalidArgumentException('DistinctProcessor requires at least one column');
        }
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            array_values(array_map(static fn(mixed $column): Reference|string => is_string($column)
                ? $column
                : $rebuild->of($column, Reference::class), type_array()->assert($data['columns'] ?? null))),
            ($data['input'] ?? null) === null ? null : Schema::fromArray(type_array()->assert($data['input'])),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, ['columns' => $this->columns, 'input' => $this->input?->normalize()]);
    }

    /**
     * Every split drops its own duplicates; the rows of one key meet in one process. null while unbound or when a
     * column is missing from the input.
     */
    public function distribution(): ?Distribution
    {
        $names = [];

        foreach ($this->columns as $column) {
            $name = is_string($column) ? $column : $column->name();

            if ($this->input?->findDefinition($name) === null) {
                return null;
            }

            $names[] = $name;
        }

        return $names === [] ? null : Distribution::clustered($names, $this);
    }

    public function bind(Schema $input): BoundStep
    {
        return new BoundStep(new self($this->columns, $input), $input);
    }

    public function breaks(): bool
    {
        return false;
    }

    /**
     * @param Generator<Rows> $rows
     *
     * @return Generator<int, Rows, null|Signal, void>
     */
    public function process(Generator $rows, Backend $backend): Generator
    {
        $keys = new KeyEncoder(References::init(...$this->columns)->names());
        $seen = new SeenKeys();

        foreach ($rows as $batch) {
            $signal = yield $batch->gather($seen->add($keys->of($batch)));

            if ($signal === Signal::STOP) {
                $rows->send(Signal::STOP);

                return;
            }
        }
    }
}
