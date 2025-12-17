<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\{Statement, StatementTrait};
use Flow\PostgreSql\Protobuf\AST\{CompositeTypeStmt, CreateAmStmt, CreateCastStmt, CreateConversionStmt, CreateDomainStmt, CreateEnumStmt, CreateEventTrigStmt, CreateExtensionStmt, CreateFdwStmt, CreateForeignServerStmt, CreateForeignTableStmt, CreateFunctionStmt, CreateOpClassStmt, CreateOpFamilyStmt, CreatePLangStmt, CreatePolicyStmt, CreatePublicationStmt, CreateRangeStmt, CreateRoleStmt, CreateSchemaStmt, CreateSeqStmt, CreateStatsStmt, CreateStmt, CreateSubscriptionStmt, CreateTableAsStmt, CreateTableSpaceStmt, CreateTransformStmt, CreateTrigStmt, CreateUserMappingStmt, CreatedbStmt, DefineStmt};

/**
 * Represents CREATE statements (CREATE TABLE, CREATE INDEX, CREATE ROLE, CREATE DATABASE, etc.).
 *
 * @implements Statement<CompositeTypeStmt|CreateAmStmt|CreateCastStmt|CreateConversionStmt|CreatedbStmt|CreateDomainStmt|CreateEnumStmt|CreateEventTrigStmt|CreateExtensionStmt|CreateFdwStmt|CreateForeignServerStmt|CreateForeignTableStmt|CreateFunctionStmt|CreateOpClassStmt|CreateOpFamilyStmt|CreatePLangStmt|CreatePolicyStmt|CreatePublicationStmt|CreateRangeStmt|CreateRoleStmt|CreateSchemaStmt|CreateSeqStmt|CreateStatsStmt|CreateStmt|CreateSubscriptionStmt|CreateTableAsStmt|CreateTableSpaceStmt|CreateTransformStmt|CreateTrigStmt|CreateUserMappingStmt|DefineStmt>
 */
final readonly class CreateStatement implements Statement
{
    use StatementTrait;

    public function __construct(
        private CreateStmt|CreateSchemaStmt|CreateSeqStmt|CreateTableAsStmt|CreateTableSpaceStmt|CreateFunctionStmt|CreateTrigStmt|CreateRoleStmt|CreateDomainStmt|CreateEnumStmt|CreateRangeStmt|CreateExtensionStmt|CreateFdwStmt|CreateForeignServerStmt|CreateForeignTableStmt|CreateUserMappingStmt|CreatePolicyStmt|CreateAmStmt|CreatePublicationStmt|CreateSubscriptionStmt|CreateStatsStmt|CreateCastStmt|CreateConversionStmt|CreateOpClassStmt|CreateOpFamilyStmt|CreatePLangStmt|CreateTransformStmt|CreateEventTrigStmt|CreatedbStmt|CompositeTypeStmt|DefineStmt $stmt,
    ) {
    }

    public function raw() : CreateStmt|CreateSchemaStmt|CreateSeqStmt|CreateTableAsStmt|CreateTableSpaceStmt|CreateFunctionStmt|CreateTrigStmt|CreateRoleStmt|CreateDomainStmt|CreateEnumStmt|CreateRangeStmt|CreateExtensionStmt|CreateFdwStmt|CreateForeignServerStmt|CreateForeignTableStmt|CreateUserMappingStmt|CreatePolicyStmt|CreateAmStmt|CreatePublicationStmt|CreateSubscriptionStmt|CreateStatsStmt|CreateCastStmt|CreateConversionStmt|CreateOpClassStmt|CreateOpFamilyStmt|CreatePLangStmt|CreateTransformStmt|CreateEventTrigStmt|CreatedbStmt|CompositeTypeStmt|DefineStmt
    {
        return $this->stmt;
    }
}
