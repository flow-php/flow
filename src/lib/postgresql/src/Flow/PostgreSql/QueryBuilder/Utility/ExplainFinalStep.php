<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Utility;

use Flow\PostgreSql\Protobuf\AST\ExplainStmt;
use Flow\PostgreSql\QueryBuilder\SqlQuery;

interface ExplainFinalStep extends SqlQuery
{
    public function analyze() : self;

    public function buffers(bool $enabled = true) : self;

    public function costs(bool $enabled = true) : self;

    public function format(ExplainFormat $format) : self;

    public function memory(bool $enabled = true) : self;

    public function settings(bool $enabled = true) : self;

    public function summary(bool $enabled = true) : self;

    public function timing(bool $enabled = true) : self;

    public function toAst() : ExplainStmt;

    public function toSql() : string;

    public function verbose() : self;

    public function wal(bool $enabled = true) : self;
}
