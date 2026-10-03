<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Php;

use Flow\ETL\Column\Column;
use Flow\ETL\Column\ColumnBuilder;
use Flow\ETL\Column\Physical\IdentityPhysical;
use Flow\ETL\Column\Physical\Physical;
use Flow\ETL\Column\Physical\PhysicalKind;
use Flow\ETL\Exception\ColumnMismatchException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Schema\Definition\NullDefinition;
use Flow\Types\Exception\Exception as TypesException;
use Flow\Types\Type;
use Flow\Types\Type\Native\BooleanType;
use Flow\Types\Type\Native\FloatType;
use Flow\Types\Type\Native\IntegerType;
use Flow\Types\Type\Native\StringType;

use function get_debug_type;
use function sprintf;

final class CastingColumnBuilder implements ColumnBuilder
{
    private readonly bool $identity;

    private readonly ?string $native;

    private readonly bool $nullable;

    /**
     * @var Type<mixed>
     */
    private readonly Type $type;

    /**
     * @param Definition<mixed> $definition
     */
    public function __construct(
        private readonly Definition $definition,
        private readonly Physical $physical,
        private readonly PhpColumnBuilder $physicals,
    ) {
        $this->identity = $physical instanceof IdentityPhysical;
        $this->nullable = $definition->isNullable() || $definition instanceof NullDefinition;
        $this->type = $definition->type();
        $this->native = $this->identity
            ? match ($this->type::class) {
                BooleanType::class => 'bool',
                IntegerType::class => 'int',
                FloatType::class => 'float',
                StringType::class => 'string',
                default => null,
            }
            : null;
    }

    public function append(mixed $value): void
    {
        if ($value === null) {
            if (!$this->nullable) {
                throw ColumnMismatchException::valueDoesNotMatch($this->definition, null);
            }

            $this->physicals->appendPhysical(null);

            return;
        }

        $this->physicals->appendPhysical($this->physical->toPhysical($this->type->cast($value)));
    }

    public function appendFrom(Column $column, int $i): void
    {
        if (!$this->nullable && $column->isNull($i)) {
            throw ColumnMismatchException::valueDoesNotMatch($this->definition, null);
        }

        $this->physicals->appendPhysical($column->at($i));
    }

    public function appendMany(array $values): void
    {
        $physicals = [];
        $nulls = 0;
        $position = 0;

        try {
            // @mago-ignore analysis:mixed-assignment
            foreach ($values as $position => $value) {
                if ($value === null) {
                    if (!$this->nullable) {
                        throw ColumnMismatchException::valueDoesNotMatch($this->definition, null);
                    }

                    $physicals[] = null;
                    $nulls++;

                    continue;
                }

                if (get_debug_type($value) === $this->native) {
                    $physicals[] = $value;

                    continue;
                }

                // @mago-ignore analysis:mixed-assignment
                $cast = $this->type->cast($value);

                if ($this->identity) {
                    $physicals[] = $cast;

                    continue;
                }

                try {
                    $physicals[] = $this->physical->toPhysical($cast);
                } catch (TypesException $e) {
                    // the value cast, so only the physical form's reason (a range) tells the user why it was refused
                    throw ColumnMismatchException::valueDoesNotMatch($this->definition, $value, $e);
                }
            }
        } catch (ColumnMismatchException $e) {
            throw new SchemaMismatchException($position, $e);
        } catch (TypesException) {
            throw new SchemaMismatchException($position, ColumnMismatchException::valueDoesNotMatch(
                $this->definition,
                $values[$position],
            ));
        }

        $this->physicals->appendPhysicals($physicals, $nulls);
    }

    /**
     * @param list<mixed> $physicals
     *
     * @throws SchemaMismatchException
     */
    public function appendPhysicals(array $physicals, ?int $nullCount = null): void
    {
        $kind = new PhysicalKind();
        $type = $this->definition->type();
        $checkNulls = !$this->nullable && $nullCount !== 0;

        // @mago-ignore analysis:mixed-assignment
        foreach ($physicals as $position => $physical) {
            if ($physical === null) {
                if ($checkNulls) {
                    throw new SchemaMismatchException($position, ColumnMismatchException::valueDoesNotMatch(
                        $this->definition,
                        null,
                    ));
                }

                continue;
            }

            if (!$kind->accepts($type, $physical)) {
                throw new InvalidArgumentException(sprintf(
                    'Cannot store a %s physical in a %s column',
                    get_debug_type($physical),
                    $type->toString(),
                ));
            }
        }

        $this->physicals->appendPhysicals($physicals, $nullCount);
    }

    public function appendTake(Column $column, array $indices): void
    {
        $all = $column->physicals();
        $physicals = [];

        foreach ($indices as $index) {
            if (!$this->nullable && $all[$index] === null) {
                throw ColumnMismatchException::valueDoesNotMatch($this->definition, null);
            }

            $physicals[] = $all[$index];
        }

        $this->physicals->appendPhysicals($physicals);
    }

    public function count(): int
    {
        return $this->physicals->count();
    }

    public function finish(): Column
    {
        return $this->physicals->finish();
    }
}
