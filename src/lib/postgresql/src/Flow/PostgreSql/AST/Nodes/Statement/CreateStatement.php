<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\Statement;
use Flow\PostgreSql\AST\Nodes\StatementTrait;
use Flow\PostgreSql\Protobuf\AST\CompositeTypeStmt;
use Flow\PostgreSql\Protobuf\AST\CreateAmStmt;
use Flow\PostgreSql\Protobuf\AST\CreateCastStmt;
use Flow\PostgreSql\Protobuf\AST\CreateConversionStmt;
use Flow\PostgreSql\Protobuf\AST\CreatedbStmt;
use Flow\PostgreSql\Protobuf\AST\CreateDomainStmt;
use Flow\PostgreSql\Protobuf\AST\CreateEnumStmt;
use Flow\PostgreSql\Protobuf\AST\CreateEventTrigStmt;
use Flow\PostgreSql\Protobuf\AST\CreateExtensionStmt;
use Flow\PostgreSql\Protobuf\AST\CreateFdwStmt;
use Flow\PostgreSql\Protobuf\AST\CreateForeignServerStmt;
use Flow\PostgreSql\Protobuf\AST\CreateForeignTableStmt;
use Flow\PostgreSql\Protobuf\AST\CreateFunctionStmt;
use Flow\PostgreSql\Protobuf\AST\CreateOpClassStmt;
use Flow\PostgreSql\Protobuf\AST\CreateOpFamilyStmt;
use Flow\PostgreSql\Protobuf\AST\CreatePLangStmt;
use Flow\PostgreSql\Protobuf\AST\CreatePolicyStmt;
use Flow\PostgreSql\Protobuf\AST\CreatePublicationStmt;
use Flow\PostgreSql\Protobuf\AST\CreateRangeStmt;
use Flow\PostgreSql\Protobuf\AST\CreateRoleStmt;
use Flow\PostgreSql\Protobuf\AST\CreateSchemaStmt;
use Flow\PostgreSql\Protobuf\AST\CreateSeqStmt;
use Flow\PostgreSql\Protobuf\AST\CreateStatsStmt;
use Flow\PostgreSql\Protobuf\AST\CreateStmt;
use Flow\PostgreSql\Protobuf\AST\CreateSubscriptionStmt;
use Flow\PostgreSql\Protobuf\AST\CreateTableAsStmt;
use Flow\PostgreSql\Protobuf\AST\CreateTableSpaceStmt;
use Flow\PostgreSql\Protobuf\AST\CreateTransformStmt;
use Flow\PostgreSql\Protobuf\AST\CreateTrigStmt;
use Flow\PostgreSql\Protobuf\AST\CreateUserMappingStmt;
use Flow\PostgreSql\Protobuf\AST\DefineStmt;

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
    ) {}

    public function raw(): CreateStmt|CreateSchemaStmt|CreateSeqStmt|CreateTableAsStmt|CreateTableSpaceStmt|CreateFunctionStmt|CreateTrigStmt|CreateRoleStmt|CreateDomainStmt|CreateEnumStmt|CreateRangeStmt|CreateExtensionStmt|CreateFdwStmt|CreateForeignServerStmt|CreateForeignTableStmt|CreateUserMappingStmt|CreatePolicyStmt|CreateAmStmt|CreatePublicationStmt|CreateSubscriptionStmt|CreateStatsStmt|CreateCastStmt|CreateConversionStmt|CreateOpClassStmt|CreateOpFamilyStmt|CreatePLangStmt|CreateTransformStmt|CreateEventTrigStmt|CreatedbStmt|CompositeTypeStmt|DefineStmt
    {
        return $this->stmt;
    }
}
