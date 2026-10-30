<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\Transformer;

use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Operators\BoundStep;
use Flow\ETL\Operators\Transformer;
use Flow\ETL\Operators\Transformer\Rename\RenameColumnStrategy;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use SensitiveParameter;

use function array_values;

final readonly class RenameEachColumnTransformer implements Transformer
{
    /**
     * @var array<RenameColumnStrategy>
     */
    private array $strategies;

    public function __construct(RenameColumnStrategy ...$strategies)
    {
        if ([] === $strategies) {
            throw new InvalidArgumentException('At least one strategy must be provided.');
        }

        $this->strategies = $strategies;
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(...array_values($rebuild->all($data['strategies'] ?? null, RenameColumnStrategy::class)));
    }

    public function describe(): ?Description
    {
        foreach ($this->strategies as $strategy) {
            if (!$strategy instanceof Describable) {
                return null;
            }
        }

        return Description::compose(self::class, ['strategies' => $this->strategies]);
    }

    public function bind(Schema $input): BoundStep
    {
        $output = $input;

        foreach ($this->strategies as $strategy) {
            foreach ($strategy->renames($output) as $from => $to) {
                $output = $output->rename((string) $from, $to);
            }
        }

        return new BoundStep($this, $output);
    }

    public function transform(Rows $rows, Backend $backend): Rows
    {
        $schema = $rows->schema();
        // strategies chain, so a later one renames what an earlier one produced
        foreach ($this->strategies as $strategy) {
            foreach ($strategy->renames($schema) as $from => $to) {
                // a numeric column name arrives as int - PHP casts numeric-string array keys
                $schema = $schema->rename((string) $from, $to);
            }
        }

        return $rows->withSchema($schema);
    }
}
