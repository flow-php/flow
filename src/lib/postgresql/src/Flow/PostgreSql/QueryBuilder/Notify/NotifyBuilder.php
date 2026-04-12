<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Notify;

use Flow\PostgreSql\Protobuf\AST\NotifyStmt;
use Flow\PostgreSql\QueryBuilder\AstToSql;

final readonly class NotifyBuilder implements NotifyFinalStep
{
    use AstToSql;

    private function __construct(
        private string $channel,
        private string $payload = '',
    ) {
    }

    public static function create(string $channel) : NotifyFinalStep
    {
        return new self($channel);
    }

    public function toAst() : NotifyStmt
    {
        $stmt = (new NotifyStmt())->setConditionname($this->channel);

        if ($this->payload !== '') {
            $stmt->setPayload($this->payload);
        }

        return $stmt;
    }

    public function withPayload(string $payload) : NotifyFinalStep
    {
        return new self($this->channel, $payload);
    }
}
