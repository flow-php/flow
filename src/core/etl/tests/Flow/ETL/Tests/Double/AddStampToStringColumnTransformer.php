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

use function Flow\ETL\DSL\array_to_rows;
use function Flow\Types\DSL\type_string;
use function sprintf;

final readonly class AddStampToStringColumnTransformer implements Transformer
{
    public function __construct(
        private string $columnName,
        private string $stamp,
        private string $divider,
    ) {}

    public function bind(Schema $input): BoundStep
    {
        return new BoundStep($this, $input);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        throw InvalidArgumentException::because('%s holds live state and is never described', self::class);
    }

    public function describe(): ?Description
    {
        return null;
    }

    public static function divideBySemicolon(string $columnName, string $stamp): self
    {
        return new self($columnName, $stamp, ':');
    }

    public function transform(Rows $rows, Backend $backend): Rows
    {
        $stamped = [];

        for ($i = 0; $i < $rows->count(); $i++) {
            $stamped[] = [
                ...$rows->values($i),
                $this->columnName => sprintf(
                    '%s%s%s',
                    type_string()->assert($rows->column($this->columnName)->value($i)),
                    $this->divider,
                    $this->stamp,
                ),
            ];
        }

        return array_to_rows($stamped, $rows->schema());
    }
}
