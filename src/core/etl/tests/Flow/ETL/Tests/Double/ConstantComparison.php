<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\Join\Condition;
use Flow\ETL\Values\Rows;
use SensitiveParameter;

use function array_fill;

/**
 * A comparison whose every pair gives the same answer.
 */
final readonly class ConstantComparison implements Condition
{
    public function __construct(
        public bool $result,
    ) {}

    public function compare(Rows $left, Rows $right): array
    {
        // @mago-ignore analysis:possibly-invalid-argument
        return $left->isEmpty() ? [] : array_fill(0, $left->count(), $this->result);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        throw InvalidArgumentException::because('%s holds live state and is never described', self::class);
    }

    public function describe(): ?Description
    {
        return null;
    }

    public function left(): array
    {
        return [];
    }

    public function right(): array
    {
        return [];
    }
}
