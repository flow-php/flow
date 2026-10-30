<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

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
use SensitiveParameter;

use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_optional;
use function preg_match_all;

final class RegexAll implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $pattern;
    private readonly ScalarFunction $subject;
    private readonly ScalarFunction $offset;

    /**
     * @param array<array-key, mixed>|ScalarFunction|string $subject
     */
    public function __construct(
        ScalarFunction|string $pattern,
        ScalarFunction|string|array $subject,
        private readonly int $flags = 0,
        ScalarFunction|int $offset = 0,
    ) {
        // PREG_OFFSET_CAPTURE and PREG_UNMATCHED_AS_NULL change each match's shape;
        // PREG_SET_ORDER transposes the whole result - all three change the column's type per flag value.
        if (
            ($flags & PREG_OFFSET_CAPTURE) !== 0
            || ($flags & PREG_UNMATCHED_AS_NULL) !== 0
            || ($flags & PREG_SET_ORDER) !== 0
        ) {
            throw new InvalidArgumentException(
                'RegexAll does not support PREG_OFFSET_CAPTURE, PREG_UNMATCHED_AS_NULL or PREG_SET_ORDER flags',
            );
        }

        $this->pattern = $pattern instanceof ScalarFunction ? $pattern : new Literal($pattern);
        $this->subject = $subject instanceof ScalarFunction ? $subject : new Literal($subject);
        $this->offset = $offset instanceof ScalarFunction ? $offset : new Literal($offset);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['pattern'] ?? null, ScalarFunction::class),
            $rebuild->of($data['subject'] ?? null, ScalarFunction::class),
            type_integer()->assert($data['flags'] ?? null),
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
        return [$this->pattern, $this->subject, $this->offset];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0], $children[1], $this->flags, $children[2]);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_optional(type_array());
    }

    public function eval(Rows $rows, Backend $backend): Column
    {
        $patterns = (new Parameter($this->pattern))->asStrings($rows, $backend);
        $subjects = (new Parameter($this->subject))->asStrings($rows, $backend);
        $offsets = (new Parameter($this->offset))->asInts($rows, $backend);
        $results = [];
        $i = 0;

        try {
            foreach ($patterns as $i => $pattern) {
                $subject = $subjects[$i];
                $offset = $offsets[$i];

                if ($pattern === null) {
                    throw new InvalidArgumentException('RegexAll requires non-null pattern');
                }

                if ($subject === null) {
                    throw new InvalidArgumentException('RegexAll requires non-null subject');
                }

                if ($offset === null) {
                    throw new InvalidArgumentException('RegexAll requires non-null offset');
                }

                $matches = [];

                // Returns the number of full pattern matches (which might be zero), or false on failure.
                if (preg_match_all($pattern, $subject, $matches, $this->flags, $offset) !== false) {
                    if ($matches === [[]]) {
                        $results[] = null;

                        continue;
                    }

                    $results[] = $matches;

                    continue;
                }

                $results[] = null;
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($backend))->of($this, $results);
    }
}
