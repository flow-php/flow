<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Operators\BoundStep;
use Flow\ETL\Operators\Processor;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Schema;
use Generator;
use SensitiveParameter;
use Throwable;

use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;

final readonly class SwallowingProcessor implements Processor
{
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

    public function breaks(): bool
    {
        return false;
    }

    public function process(Generator $rows, Backend $backend): Generator
    {
        try {
            foreach ($rows as $batch) {
                yield $batch;
            }
        } catch (Throwable) {
            yield rows(schema());
        }
    }
}
