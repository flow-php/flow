<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\Transformer;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\DataDependentSchemaException;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Telemetry\SpanAttributes;
use Flow\ETL\Foundation\Telemetry\TelemetryAttributes;
use Flow\ETL\Functions\Join\Expression;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\ExtractorFactory;
use Flow\ETL\Kernels\HashJoin\Joiner;
use Flow\ETL\Kernels\HashJoin\JoinSide;
use Flow\ETL\Operators\BoundStep;
use Flow\ETL\Operators\Transformer;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Join\JoinType;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Generator;
use SensitiveParameter;

use function iterator_to_array;

final readonly class JoinEachRowsTransformer implements Transformer, SpanAttributes
{
    private function __construct(
        private ExtractorFactory $factory,
        private Expression $expression,
        private JoinType $type,
    ) {}

    public function bind(Schema $input): BoundStep
    {
        throw DataDependentSchemaException::step(
            self::class,
            "its right side is built from each left batch's row values",
        );
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        throw InvalidArgumentException::because('%s holds live state and is never described', self::class);
    }

    public function describe(): ?Description
    {
        return null;
    }

    public static function inner(ExtractorFactory $right, Expression $condition): self
    {
        return new self($right, $condition, JoinType::inner);
    }

    public static function left(ExtractorFactory $right, Expression $condition): self
    {
        return new self($right, $condition, JoinType::left);
    }

    public static function leftAnti(ExtractorFactory $right, Expression $condition): self
    {
        return new self($right, $condition, JoinType::left_anti);
    }

    public static function right(ExtractorFactory $right, Expression $condition): self
    {
        return new self($right, $condition, JoinType::right);
    }

    /**
     * @throws InvalidArgumentException
     */
    public function spanAttributes(): array
    {
        return [TelemetryAttributes::ATTR_JOIN_TYPE => $this->type->value];
    }

    public function transform(Rows $rows, Backend $backend): Rows
    {
        $right = $this->factory->from($rows);
        $batches = iterator_to_array($right->extract($backend, new Criteria()), false);
        $rightRows = $batches === []
            ? Rows::empty($right->schema(), $backend)
            : Rows::concat($batches[0]->schema(), $backend, ...$batches);

        $schema = $this->expression->schema($this->type);
        $single = static function (Rows $rows): Generator {
            yield $rows;
        };

        return Rows::concat(
            $schema->output($rows->schema(), $rightRows->schema()),
            $backend,
            ...iterator_to_array(
                (new Joiner($this->type, $schema, $this->expression->keys(), $this->expression, $backend))->join(
                    JoinSide::of($single($rows), $rows->schema()),
                    JoinSide::of($single($rightRows), $rightRows->schema()),
                ),
                false,
            ),
        );
    }
}
