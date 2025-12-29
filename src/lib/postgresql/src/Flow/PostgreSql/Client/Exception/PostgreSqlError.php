<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Exception;

final readonly class PostgreSqlError
{
    private function __construct(
        public string $sqlState,
        public PostgreSqlErrorCategory $category,
        public string $message,
        public ?string $detail,
        public ?string $hint,
        public ?string $schema,
        public ?string $table,
        public ?string $column,
        public ?string $constraint,
        public ?int $position,
    ) {
    }

    public static function fromDiagnostics(
        string $sqlState,
        string $message,
        ?string $detail = null,
        ?string $hint = null,
        ?string $schema = null,
        ?string $table = null,
        ?string $column = null,
        ?string $constraint = null,
        ?int $position = null,
    ) : self {
        return new self(
            $sqlState,
            PostgreSqlErrorCategory::fromSqlState($sqlState),
            $message,
            $detail,
            $hint,
            $schema,
            $table,
            $column,
            $constraint,
            $position,
        );
    }

    public static function unknown(string $message = 'Unknown error') : self
    {
        return new self(
            '00000',
            PostgreSqlErrorCategory::UNKNOWN,
            $message,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
        );
    }

    public function fullMessage() : string
    {
        return $this->message;
    }

    public function isCheckViolation() : bool
    {
        return $this->sqlState === '23514';
    }

    public function isConnectionError() : bool
    {
        return $this->category === PostgreSqlErrorCategory::CONNECTION_EXCEPTION;
    }

    public function isDataError() : bool
    {
        return $this->category === PostgreSqlErrorCategory::DATA_EXCEPTION;
    }

    public function isDeadlockDetected() : bool
    {
        return $this->sqlState === '40P01';
    }

    public function isExclusionViolation() : bool
    {
        return $this->sqlState === '23P01';
    }

    public function isForeignKeyViolation() : bool
    {
        return $this->sqlState === '23503';
    }

    public function isIntegrityViolation() : bool
    {
        return $this->category === PostgreSqlErrorCategory::INTEGRITY_CONSTRAINT_VIOLATION;
    }

    public function isNotNullViolation() : bool
    {
        return $this->sqlState === '23502';
    }

    public function isSerializationFailure() : bool
    {
        return $this->sqlState === '40001';
    }

    public function isSyntaxError() : bool
    {
        return $this->category === PostgreSqlErrorCategory::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION;
    }

    public function isTransactionRollback() : bool
    {
        return $this->category === PostgreSqlErrorCategory::TRANSACTION_ROLLBACK;
    }

    public function isUniqueViolation() : bool
    {
        return $this->sqlState === '23505';
    }

    public function safeMessage() : string
    {
        return $this->category->safeMessage();
    }
}
