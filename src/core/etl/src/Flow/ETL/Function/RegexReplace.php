<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Exception;
use Flow\ETL\Column\Column;
use Flow\ETL\Exception\EvaluationException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Evaluation\ResultColumn;
use Flow\ETL\Rows;
use Flow\Types\Type;

use function count;
use function Flow\ETL\DSL\lit;
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
        $this->pattern = $pattern instanceof ScalarFunction ? $pattern : lit($pattern);
        $this->replacement = $replacement instanceof ScalarFunction ? $replacement : lit($replacement);
        $this->subject = $subject instanceof ScalarFunction ? $subject : lit($subject);
        $this->limit = $limit === null ? null : ($limit instanceof ScalarFunction ? $limit : lit($limit));
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

    public function eval(Rows $rows, FlowContext $context): Column
    {
        $patterns = (new Parameter($this->pattern))->asStrings($rows, $context);
        $replacements = (new Parameter($this->replacement))->asStrings($rows, $context);
        $subjects = (new Parameter($this->subject))->asStrings($rows, $context);
        $limits = (new Parameter($this->limit))->asInts($rows, $context);
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

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}
