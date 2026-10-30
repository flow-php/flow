<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use DateTimeImmutable;
use DateTimeZone;
use Exception;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\EvaluationException;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\Evaluation\ResultColumn;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;
use Flow\Types\Type\Logical\DateTimeType;
use SensitiveParameter;

use function Flow\Types\DSL\type_datetime;

final class Now implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $timeZone;

    private readonly DateTimeType $type;

    public function __construct(ScalarFunction|DateTimeZone $timeZone = new DateTimeZone('UTC'))
    {
        $this->timeZone = $timeZone instanceof ScalarFunction ? $timeZone : new Literal($timeZone);
        // @mago-ignore analysis:mixed-assignment
        $zone = $this->timeZone instanceof Literal ? $this->timeZone->value() : null;
        $this->type = $zone instanceof DateTimeZone ? type_datetime($zone) : type_datetime();
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self($rebuild->of($data['time_zone'] ?? null, ScalarFunction::class));
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'time_zone' => $this->timeZone,
        ]);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->timeZone];
    }

    public function deterministic(): bool
    {
        return false;
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0]);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return $this->type;
    }

    public function eval(Rows $rows, Backend $backend): Column
    {
        $tzes = (new Parameter($this->timeZone))->asInstancesOf($rows, $backend, DateTimeZone::class);
        $results = [];
        $i = 0;

        try {
            foreach ($tzes as $i => $tz) {
                if ($tz === null) {
                    throw new InvalidArgumentException('Now function requires valid DateTimeZone');
                }

                $results[] = $this->type->cast(new DateTimeImmutable('now', $tz));
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($backend))->of($this, $results);
    }
}
