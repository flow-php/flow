<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Operators\BoundStep;
use Flow\ETL\Operators\Transformer;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use SensitiveParameter;
use Throwable;

final readonly class ThrowWhenRowMatches implements Transformer
{
    public function __construct(
        private string $column,
        private mixed $value,
        private Throwable $throwable,
    ) {}

    public function transform(Rows $rows, Backend $backend): Rows
    {
        foreach ($rows->toArray() as $row) {
            if ($row[$this->column] === $this->value) {
                throw $this->throwable;
            }
        }

        return $rows;
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        throw InvalidArgumentException::because('%s holds live state and is never described', self::class);
    }

    public function describe(): ?Description
    {
        return null;
    }

    public function bind(Schema $input): BoundStep
    {
        return new BoundStep($this, $input);
    }
}
