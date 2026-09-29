<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\BoundStep;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Transformer;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\Types\DSL\type_string;
use function sprintf;

final readonly class AddStampToStringEntryTransformer implements Transformer
{
    public function __construct(
        private string $entryName,
        private string $stamp,
        private string $divider,
    ) {}

    public function bind(Schema $input): BoundStep
    {
        return new BoundStep($this, $input);
    }

    public static function divideBySemicolon(string $entryName, string $stamp): self
    {
        return new self($entryName, $stamp, ':');
    }

    public function transform(Rows $rows, FlowContext $context): Rows
    {
        $stamped = [];

        for ($i = 0; $i < $rows->count(); $i++) {
            $stamped[] = [
                ...$rows->values($i),
                $this->entryName => sprintf(
                    '%s%s%s',
                    type_string()->assert($rows->column($this->entryName)->value($i)),
                    $this->divider,
                    $this->stamp,
                ),
            ];
        }

        return array_to_rows($stamped, $rows->schema());
    }
}
