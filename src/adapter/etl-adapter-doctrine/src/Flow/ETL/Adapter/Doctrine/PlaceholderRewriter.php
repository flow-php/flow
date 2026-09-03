<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Doctrine\DBAL\SQL\Parser;
use Doctrine\DBAL\SQL\Parser\Visitor;

use function implode;

/**
 * @internal implements a DBAL-internal Visitor; use NativePlaceholders instead
 */
final class PlaceholderRewriter implements Visitor
{
    /**
     * @var list<string>
     */
    private array $buffer = [];

    /**
     * @var int<0, max>
     */
    private int $count = 0;

    public function __construct(
        private readonly PlaceholderDialect $dialect,
    ) {}

    public function acceptNamedParameter(string $sql): void
    {
        $this->count += 1;
        $this->buffer[] = $this->dialect->placeholder($this->count);
    }

    public function acceptOther(string $sql): void
    {
        $this->buffer[] = $sql;
    }

    public function acceptPositionalParameter(string $sql): void
    {
        $this->count += 1;
        $this->buffer[] = $this->dialect->placeholder($this->count);
    }

    public function rewrite(string $sql): RewrittenSql
    {
        $this->buffer = [];
        $this->count = 0;

        (new Parser($this->dialect->usesBackslashEscapes()))->parse($sql, $this);

        return new RewrittenSql(implode('', $this->buffer), $this->count);
    }
}
