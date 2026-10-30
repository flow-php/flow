<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Exception;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\EvaluationException;
use Flow\ETL\Functions\Evaluation\ResultColumn;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;
use Flow\Types\Type\Nullability;
use SensitiveParameter;

use function Flow\Types\DSL\type_boolean;
use function preg_match;

final class RegexMatch implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $pattern;
    private readonly ScalarFunction $subject;
    private readonly ScalarFunction $flags;
    private readonly ScalarFunction $offset;

    /**
     * @param array<array-key, mixed>|ScalarFunction|string $subject
     */
    public function __construct(
        ScalarFunction|string $pattern,
        ScalarFunction|string|array $subject,
        ScalarFunction|int $flags = 0,
        ScalarFunction|int $offset = 0,
    ) {
        $this->pattern = $pattern instanceof ScalarFunction ? $pattern : new Literal($pattern);
        $this->subject = $subject instanceof ScalarFunction ? $subject : new Literal($subject);
        $this->flags = $flags instanceof ScalarFunction ? $flags : new Literal($flags);
        $this->offset = $offset instanceof ScalarFunction ? $offset : new Literal($offset);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['pattern'] ?? null, ScalarFunction::class),
            $rebuild->of($data['subject'] ?? null, ScalarFunction::class),
            $rebuild->of($data['flags'] ?? null, ScalarFunction::class),
            $rebuild->of($data['offset'] ?? null, ScalarFunction::class),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'pattern' => $this->pattern,
            'subject' => $this->subject,
            'flags' => $this->flags,
            'offset' => $this->offset,
        ]);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->pattern, $this->subject, $this->flags, $this->offset];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0], $children[1], $children[2], $children[3]);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return (new Nullability())->any(
            type_boolean(),
            $this->pattern->returns(),
            $this->subject->returns(),
            $this->flags->returns(),
            $this->offset->returns(),
        );
    }

    public function eval(Rows $rows, Backend $backend): Column
    {
        $patterns = (new Parameter($this->pattern))->asStrings($rows, $backend);
        $subjects = (new Parameter($this->subject))->asStrings($rows, $backend);
        $flagsList = (new Parameter($this->flags))->asInts($rows, $backend);
        $offsets = (new Parameter($this->offset))->asInts($rows, $backend);
        $results = [];
        $i = 0;

        try {
            foreach ($patterns as $i => $pattern) {
                $subject = $subjects[$i];
                $flags = $flagsList[$i];
                $offset = $offsets[$i];

                if ($pattern === null || $subject === null || $flags === null || $offset === null) {
                    $results[] = null;

                    continue;
                }

                $results[] = preg_match(pattern: $pattern, subject: $subject, flags: $flags, offset: $offset) === 1;
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($backend))->of($this, $results);
    }
}
