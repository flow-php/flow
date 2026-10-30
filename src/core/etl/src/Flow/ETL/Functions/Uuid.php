<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use DateTimeInterface;
use Exception;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\EvaluationException;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\Functions\Evaluation\ResultColumn;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;
use Flow\Types\Value\Uuid as FlowUuid;
use Ramsey\Uuid\Uuid as RamseyUuid;
use Ramsey\Uuid\UuidInterface;
use SensitiveParameter;
use Symfony\Component\Uid\Uuid as SymfonyUuid;
use Symfony\Component\Uid\UuidV4;
use Symfony\Component\Uid\UuidV7;

use function class_exists;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_uuid;

if (!class_exists(RamseyUuid::class) && !class_exists(SymfonyUuid::class)) {
    throw new RuntimeException(
        "\Ramsey\Uuid\Uuid nor \Symfony\Component\Uid\Uuid class not found, please add 'ramsey/uuid' or 'symfony/uid' as a dependency to the project first.",
    );
}

final class Uuid implements ScalarFunction
{
    use ScalarFunctionChain;

    /**
     * Null means "uuid4 - no operand", not lit(null).
     */
    private readonly ?ScalarFunction $value;

    private function __construct(
        private readonly string $uuidVersion,
        ScalarFunction|DateTimeInterface|null $value = null,
    ) {
        $this->value = $value === null ? null : ($value instanceof ScalarFunction ? $value : new Literal($value));
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            type_string()->assert($data['version'] ?? null),
            ($data['value'] ?? null) === null ? null : $rebuild->of($data['value'], ScalarFunction::class),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, ['version' => $this->uuidVersion, 'value' => $this->value]);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return $this->value === null ? [] : [$this->value];
    }

    public function deterministic(): bool
    {
        // both generate: uuid7 over the same timestamp still draws a new random tail
        return false;
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($this->uuidVersion, $children[0] ?? null);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_uuid();
    }

    public static function uuid4(): self
    {
        return new self('uuid4');
    }

    public static function uuid7(ScalarFunction|DateTimeInterface $value): self
    {
        return new self('uuid7', $value);
    }

    public function eval(Rows $rows, Backend $backend): Column
    {
        $results = [];

        if ($this->uuidVersion === 'uuid4') {
            for ($i = 0, $count = $rows->count(); $i < $count; $i++) {
                $results[] = new FlowUuid($this->generateV4());
            }

            return (new ResultColumn($backend))->of($this, $results);
        }

        $params = (new Parameter($this->value))->asTypes(
            $rows,
            $backend,
            type_string(),
            type_instance_of(DateTimeInterface::class),
        );
        $i = 0;

        try {
            foreach ($params as $i => $param) {
                if (!$param instanceof DateTimeInterface) {
                    throw new InvalidArgumentException('Uuid uuid7 function requires a DateTimeInterface value');
                }

                $results[] = new FlowUuid($this->generateV7($param));
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($backend))->of($this, $results);
    }

    private function generateV4(): UuidV4|UuidInterface
    {
        if (class_exists(RamseyUuid::class)) {
            return RamseyUuid::uuid4();
        }

        return UuidV4::v4();
    }

    private function generateV7(DateTimeInterface $dateTime): UuidV7|UuidInterface
    {
        if (class_exists(RamseyUuid::class)) {
            return RamseyUuid::uuid7($dateTime);
        }

        return new UuidV7(UuidV7::generate($dateTime));
    }
}
