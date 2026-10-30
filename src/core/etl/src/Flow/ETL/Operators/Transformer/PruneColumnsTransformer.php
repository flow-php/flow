<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\Transformer;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\Functions\Reference\References;
use Flow\ETL\Operators\BoundStep;
use Flow\ETL\Operators\Transformer;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use SensitiveParameter;

/**
 * Unlike {@see SelectColumnsTransformer}, which throws when a reference is not declared by the
 * schema, prune keeps what is there and silently skips absent references - pruned rows are spilled
 * to storage, where the pruning is opportunistic rather than a contract.
 */
final readonly class PruneColumnsTransformer implements Transformer
{
    private References $refs;

    public function __construct(string|Reference ...$refs)
    {
        $this->refs = References::init(...$refs);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(...$rebuild->of($data['refs'] ?? null, References::class)->all());
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, ['refs' => $this->refs]);
    }

    public function bind(Schema $input): BoundStep
    {
        $present = [];

        foreach ($this->refs as $ref) {
            if ($input->findDefinition($ref->name()) !== null) {
                $present[] = $ref->name();
            }
        }

        return new BoundStep($this, $input->keep(...$present)->reorder(...$present));
    }

    public function transform(Rows $rows, Backend $backend): Rows
    {
        $present = [];

        foreach ($this->refs as $ref) {
            if ($rows->schema()->findDefinition($ref->name()) !== null) {
                $present[] = $ref->name();
            }
        }

        return $rows->project($rows->schema()->keep(...$present)->reorder(...$present), $backend);
    }
}
