<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\Transformer;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\Functions\Reference\UnresolvedReference;
use Flow\ETL\Operators\BoundStep;
use Flow\ETL\Operators\Transformer;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use SensitiveParameter;

use function array_key_exists;
use function array_map;
use function array_values;
use function sprintf;

final readonly class SelectColumnsTransformer implements Transformer
{
    /**
     * @var list<Reference> in call order, never deduplicated: two references that output one name are refused at bind
     */
    private array $refs;

    public function __construct(string|Reference ...$refs)
    {
        $this->refs = array_map(UnresolvedReference::init(...), array_values($refs));
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(...$rebuild->all($data['refs'] ?? null, Reference::class));
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, ['refs' => $this->refs]);
    }

    public function bind(Schema $input): BoundStep
    {
        return new BoundStep($this, $this->output($input));
    }

    public function transform(Rows $rows, Backend $backend): Rows
    {
        $output = $this->output($rows->schema());
        $columns = [];

        foreach ($this->refs as $ref) {
            $columns[$ref->name()] = $rows->column($ref->to());
        }

        return Rows::fromColumns($output, $columns, $rows->count())->matchTo($output, $backend);
    }

    /**
     * Each reference's column, named by its alias: select(ref('a')->as('b')) outputs a's values as b.
     *
     * @throws InvalidArgumentException two references output the same name
     */
    private function output(Schema $input): Schema
    {
        $definitions = [];

        foreach ($this->refs as $ref) {
            if (array_key_exists($ref->name(), $definitions)) {
                throw new InvalidArgumentException(sprintf('select() would output column "%s" twice', $ref->name()));
            }

            $definitions[$ref->name()] = $input->get($ref->to())->rename($ref->name());
        }

        return new Schema(...array_values($definitions));
    }
}
