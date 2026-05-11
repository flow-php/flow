<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Exception;

enum PostgreSqlErrorCategory: string
{
    case CARDINALITY_VIOLATION = '21';
    case CASE_NOT_FOUND = '20';
    case CONFIGURATION_FILE_ERROR = 'F0';
    case CONNECTION_EXCEPTION = '08';
    case DATA_EXCEPTION = '22';
    case DEPENDENT_PRIVILEGE_DESCRIPTORS = '2B';
    case DIAGNOSTICS_EXCEPTION = '0Z';
    case EXTERNAL_ROUTINE_EXCEPTION = '38';
    case EXTERNAL_ROUTINE_INVOCATION_EXCEPTION = '39';
    case FDW_ERROR = 'HV';
    case FEATURE_NOT_SUPPORTED = '0A';
    case INSUFFICIENT_RESOURCES = '53';
    case INTEGRITY_CONSTRAINT_VIOLATION = '23';
    case INTERNAL_ERROR = 'XX';
    case INVALID_AUTHORIZATION_SPECIFICATION = '28';
    case INVALID_CATALOG_NAME = '3D';
    case INVALID_CURSOR_NAME = '34';
    case INVALID_CURSOR_STATE = '24';
    case INVALID_GRANTOR = '0L';
    case INVALID_ROLE_SPECIFICATION = '0P';
    case INVALID_SCHEMA_NAME = '3F';
    case INVALID_SQL_STATEMENT_NAME = '26';
    case INVALID_TRANSACTION_INITIATION = '0B';
    case INVALID_TRANSACTION_STATE = '25';
    case INVALID_TRANSACTION_TERMINATION = '2D';
    case LOCATOR_EXCEPTION = '0F';
    case NO_DATA = '02';
    case OBJECT_NOT_IN_PREREQUISITE_STATE = '55';
    case OPERATOR_INTERVENTION = '57';
    case PL_PGSQL_ERROR = 'P0';
    case PROGRAM_LIMIT_EXCEEDED = '54';
    case SAVEPOINT_EXCEPTION = '3B';
    case SNAPSHOT_FAILURE = '72';
    case SQL_ROUTINE_EXCEPTION = '2F';
    case SQL_STATEMENT_NOT_YET_COMPLETE = '03';
    case SUCCESSFUL_COMPLETION = '00';
    case SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION = '42';
    case SYSTEM_ERROR = '58';
    case TRANSACTION_ROLLBACK = '40';
    case TRIGGERED_ACTION_EXCEPTION = '09';
    case TRIGGERED_DATA_CHANGE_VIOLATION = '27';
    case UNKNOWN = '??';
    case WARNING = '01';
    case WITH_CHECK_OPTION_VIOLATION = '44';

    public static function fromSqlState(string $sqlState): self
    {
        if (\strlen($sqlState) < 2) {
            return self::UNKNOWN;
        }

        $class = \substr($sqlState, 0, 2);

        return self::tryFrom($class) ?? self::UNKNOWN;
    }

    public function isRecoverable(): bool
    {
        return match ($this) {
            self::TRANSACTION_ROLLBACK => true,
            default => false,
        };
    }

    public function safeMessage(): string
    {
        return match ($this) {
            self::SUCCESSFUL_COMPLETION => 'Operation completed successfully',
            self::WARNING => 'Operation completed with warnings',
            self::NO_DATA => 'No data found',
            self::SQL_STATEMENT_NOT_YET_COMPLETE => 'SQL statement not yet complete',
            self::CONNECTION_EXCEPTION => 'Database connection error occurred',
            self::TRIGGERED_ACTION_EXCEPTION => 'Triggered action error',
            self::FEATURE_NOT_SUPPORTED => 'Feature not supported',
            self::INVALID_TRANSACTION_INITIATION => 'Invalid transaction initiation',
            self::LOCATOR_EXCEPTION => 'Locator error',
            self::INVALID_GRANTOR => 'Invalid grantor',
            self::INVALID_ROLE_SPECIFICATION => 'Invalid role specification',
            self::DIAGNOSTICS_EXCEPTION => 'Diagnostics error',
            self::CASE_NOT_FOUND => 'Case not found',
            self::CARDINALITY_VIOLATION => 'Cardinality violation',
            self::DATA_EXCEPTION => 'Invalid data format or value',
            self::INTEGRITY_CONSTRAINT_VIOLATION => 'Data constraint violation',
            self::INVALID_CURSOR_STATE => 'Invalid cursor state',
            self::INVALID_TRANSACTION_STATE => 'Invalid transaction state',
            self::INVALID_SQL_STATEMENT_NAME => 'Invalid SQL statement name',
            self::TRIGGERED_DATA_CHANGE_VIOLATION => 'Triggered data change violation',
            self::INVALID_AUTHORIZATION_SPECIFICATION => 'Authorization error',
            self::DEPENDENT_PRIVILEGE_DESCRIPTORS => 'Dependent privilege descriptors still exist',
            self::INVALID_TRANSACTION_TERMINATION => 'Invalid transaction termination',
            self::SQL_ROUTINE_EXCEPTION => 'SQL routine error',
            self::INVALID_CURSOR_NAME => 'Invalid cursor name',
            self::EXTERNAL_ROUTINE_EXCEPTION => 'External routine error',
            self::EXTERNAL_ROUTINE_INVOCATION_EXCEPTION => 'External routine invocation error',
            self::SAVEPOINT_EXCEPTION => 'Savepoint error',
            self::INVALID_CATALOG_NAME => 'Invalid catalog name',
            self::INVALID_SCHEMA_NAME => 'Invalid schema name',
            self::TRANSACTION_ROLLBACK => 'Transaction was rolled back',
            self::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION => 'Query syntax or permission error',
            self::WITH_CHECK_OPTION_VIOLATION => 'Check option violation',
            self::INSUFFICIENT_RESOURCES => 'Server resource limit reached',
            self::PROGRAM_LIMIT_EXCEEDED => 'Program limit exceeded',
            self::OBJECT_NOT_IN_PREREQUISITE_STATE => 'Object not in prerequisite state',
            self::OPERATOR_INTERVENTION => 'Operator intervention',
            self::SYSTEM_ERROR => 'Database system error',
            self::SNAPSHOT_FAILURE => 'Snapshot failure',
            self::CONFIGURATION_FILE_ERROR => 'Configuration file error',
            self::FDW_ERROR => 'Foreign data wrapper error',
            self::PL_PGSQL_ERROR => 'PL/pgSQL error',
            self::INTERNAL_ERROR => 'Internal database error',
            self::UNKNOWN => 'Database operation failed',
        };
    }
}
