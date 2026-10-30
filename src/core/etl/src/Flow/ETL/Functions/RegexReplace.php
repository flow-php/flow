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

use function count;
use function Flow\Types\DSL\type_string;
use function preg_replace;

final class RegexReplace implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $pattern;

    private readonly ScalarFunction $replacement;

    private readonly ScalarFunction $subject;

    /**
     * Null means "unlimited" - never lit(null), so an absent limit stays distinguishable.
     */
    private readonly ?ScalarFunction $limit;

    public function __construct(
        ScalarFunction|string $pattern,
        ScalarFunction|string $replacement,
        ScalarFunction|string $subject,
        ScalarFunction|int|null $limit = null,
    ) {
        $this->pattern = $pattern instanceof ScalarFunction ? $pattern : new Literal($pattern);
        $this->replacement = $replacement instanceof ScalarFunction ? $replacement : new Literal($replacement);
        $this->subject = $subject instanceof ScalarFunction ? $subject : new Literal($subject);
        $this->limit = $limit === null ? null : ($limit instanceof ScalarFunction ? $limit : new Literal($limit));
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            $rebuild->of($data['pattern'] ?? null, ScalarFunction::class),
            $rebuild->of($data['replacement'] ?? null, ScalarFunction::class),
            $rebuild->of($data['subject'] ?? null, ScalarFunction::class),
            ($data['limit'] ?? null) === null ? null : $rebuild->of($data['limit'], ScalarFunction::class),
        );
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'pattern' => $this->pattern,
            'replacement' => $this->replacement,
            'subject' => $this->subject,
            'limit' => $this->limit,
        ]);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return (
            $this->limit === null
                ? [$this->pattern, $this->replacement, $this->subject]
                : [$this->pattern, $this->replacement, $this->subject, $this->limit]
        );
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return count($children) === 4
            ? new self($children[0], $children[1], $children[2], $children[3])
            : new self($children[0], $children[1], $children[2]);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_string();
    }

    public function eval(Rows $rows, Backend $backend): Column
    {
        $patterns = (new Parameter($this->pattern))->asStrings($rows, $backend);
        $replacements = (new Parameter($this->replacement))->asStrings($rows, $backend);
        $subjects = (new Parameter($this->subject))->asStrings($rows, $backend);
        $limits = (new Parameter($this->limit))->asInts($rows, $backend);
        $results = [];
        $i = 0;

        try {
            foreach ($patterns as $i => $pattern) {
                $replacement = $replacements[$i];
                $subject = $subjects[$i];
                $limit = $limits[$i];

                $limit = $this->limit !== null ? $limit : -1;

                if ($pattern === null) {
                    throw new InvalidArgumentException('RegexReplace requires non-null pattern');
                }

                if ($replacement === null) {
                    throw new InvalidArgumentException('RegexReplace requires non-null replacement');
                }

                if ($subject === null) {
                    throw new InvalidArgumentException('RegexReplace requires non-null subject');
                }

                if ($limit === null) {
                    throw new InvalidArgumentException('RegexReplace requires non-null limit');
                }

                $results[] = preg_replace($pattern, $replacement, $subject, $limit);
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($backend))->of($this, $results);
    }
}
