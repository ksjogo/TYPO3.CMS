<?php
declare(strict_types=1);
namespace TYPO3\CMS\Core\Database;

/*
 * This file is part of the TYPO3 CMS project.
 *
 * It is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, either version 2
 * of the License, or any later version.
 *
 * For the full copyright and license information, please read the
 * LICENSE.txt file that was distributed with this source code.
 *
 * The TYPO3 project - inspiring people to share!
 */

use TYPO3\CMS\Core\Utility\GeneralUtility;
use Doctrine\DBAL\Driver\Statement;
use TYPO3\CMS\Core\Database\Query\QueryBuilder;

class MssqlConnection extends Connection {

    static $blobTypes = null;

    protected $nextUpdatePrefix = null;
    protected $nextUpdateDistinct = false;

    private function fixDataTypes($tableName, &$data, &$types)
    {
        if(self::$blobTypes === null) {
            self::$blobTypes = [];
            $rows = $this->fetchAll('SELECT table_name, column_name FROM information_schema.columns where data_type = \'VARBINARY\'');
            foreach($rows as $row) {
                self::$blobTypes[$row['table_name']][] = $row['column_name'];
            }
        }
        if (isset(self::$blobTypes[$tableName])) {
            foreach(self::$blobTypes[$tableName] as $toFix){
                if (array_key_exists($toFix, $data)) {
                    $types[$toFix] = self::PARAM_LOB;
                }
            }
        }
    }

    /**
     * Inserts a table row with specified data.
     *
     * All SQL identifiers are expected to be unquoted and will be quoted when building the query.
     * Table expression and columns are not escaped and are not safe for user-input.
     *
     * @param string $tableName The name of the table to insert data into.
     * @param array $data An associative array containing column-value pairs.
     * @param array $types Types of the inserted data.
     *
     * @return int The number of affected rows.
     */
    public function insert($tableName, array $data, array $types = []): int
    {
        $this->fixDataTypes($tableName, $data, $types);

        //FIXME: cache that
        $identityColumn = $this->fetchAll(sprintf('SELECT name FROM sys.identity_columns WHERE OBJECT_NAME(object_id) = \'%s\'', $tableName));
        if (count($identityColumn) == 1 && array_key_exists($identityColumn[0]['name'], $data)) {
            $this->nextUpdatePrefix = sprintf('SET IDENTITY_INSERT [%s] ON; ', $tableName);
        }

        return parent::insert($tableName, $data, $types);
    }

    /**
     * Executes an SQL UPDATE statement on a table.
     *
     * All SQL identifiers are expected to be unquoted and will be quoted when building the query.
     * Table expression and columns are not escaped and are not safe for user-input.
     *
     * @param string $tableName The name of the table to update.
     * @param array $data An associative array containing column-value pairs.
     * @param array $identifier The update criteria. An associative array containing column-value pairs.
     * @param array $types Types of the merged $data and $identifier arrays in that order.
     *
     * @return int The number of affected rows.
     */
    public function update($tableName, array $data, array $identifier, array $types = []): int
    {
        $this->fixDataTypes($tableName, $data, $types);

        return parent::update($tableName, $data, $identifier, $types);
    }

    /**
     * Executes an SQL INSERT/UPDATE/DELETE query with the given parameters
     * and returns the number of affected rows.
     *
     * This method supports PDO binding types as well as DBAL mapping types.
     *
     * @param string $query  The SQL query.
     * @param array  $params The query parameters.
     * @param array  $types  The parameter types.
     *
     * @return integer The number of affected rows.
     *
     * @throws \Doctrine\DBAL\DBALException
     */
    public function executeUpdate($query, array $params = array(), array $types = array())
    {
        if ($this->nextUpdatePrefix !== null) {
            $query = $this->nextUpdatePrefix . $query;
            $this->nextUpdatePrefix = null;
        }

        return parent::executeUpdate($query, $params, $types);
    }

    /**
     * Executes an SQL SELECT statement on a table.
     *
     * All SQL identifiers are expected to be unquoted and will be quoted when building the query.
     * Table expression and columns are not escaped and are not safe for user-input.
     *
     * @param string[] $columns The columns of the table which to select.
     * @param string $tableName The name of the table on which to select.
     * @param array $identifiers The selection criteria. An associative array containing column-value pairs.
     * @param string[] $groupBy The columns to group the results by.
     * @param array $orderBy Associative array of column name/sort directions pairs.
     * @param int $limit The maximum number of rows to return.
     * @param int $offset The first result row to select (when used with limit)
     *
     * @return Statement The executed statement.
     */
    public function select(
        array $columns,
        string $tableName,
        array $identifiers = [],
        array $groupBy = [],
        array $orderBy = [],
        int $limit = 0,
        int $offset = 0
    ): Statement {
        $groupBys = array_unique($groupBy);
        if (count($groupBys) == 1 && preg_match('/\[.*?\]\.\[uid\]/', $groupBys[0])) {
            $groupBy = [];
            $this->nextQueryDistinct = true;
        }

        if (length($groupBy) > 0)
            foreach($orderBy as $column => $direction)
                if (!in_array($column, $groupBy))
                    $groupBy[] = $column;

        return parent::select($columns, $tableName, $identifiers, $groupBy, $orderBy, $limit, $offset);
    }

    /**
     * Executes an, optionally parametrized, SQL query.
     *
     * If the query is parametrized, a prepared statement is used.
     * If an SQLLogger is configured, the execution is logged.
     *
     * @param string                                      $query  The SQL query to execute.
     * @param array                                       $params The parameters to bind to the query, if any.
     * @param array                                       $types  The types the previous parameters are in.
     * @param \Doctrine\DBAL\Cache\QueryCacheProfile|null $qcp    The query cache profile, optional.
     *
     * @return \Doctrine\DBAL\Driver\Statement The executed statement.
     *
     * @throws \Doctrine\DBAL\DBALException
     */
    public function executeQuery($query, array $params = array(), $types = array(), QueryCacheProfile $qcp = null)
    {
        if ($this->nextQueryDistinct) {
            $query = preg_replace('/^SELECT /', 'SELECT DISTINCT ', $query);
            $this->nextQueryDistinct = false;
        }
        return parent::executeQuery($query, $params, $types, $qcp);
    }

    /**
     * Allow as specific database connection to fix this query while it is not stringified yet
     * @return void
     */
    public function fixQueryForSpecificConnection(QueryBuilder $query) {
        if(count($query->getConcreteQueryBuilder()->getQueryParts()['groupBy'])>0)
        {
            $groupBys = $query->getConcreteQueryBuilder()->getQueryParts()['groupBy'];
            $groupBys = array_unique($groupBys);
            if (count($groupBys) == 1 && preg_match('/\[.*?\]\.\[uid\]/', $groupBys[0])) {
                $this->nextQueryDistinct = true;
                $query->resetQueryPart('groupBy');
            }
        }
    }

    /**
     * Quotes a given input parameter.
     *
     * @param mixed       $input The parameter to be quoted.
     * @param string|null $type  The type of the parameter.
     *
     * @return string The quoted parameter.
     */
    public function quote($input, $type = null) : string
    {
        $quoted = parent::quote($input, $type);
        //the MSSQL Doctrine Driver is not always returning strings, but the TYPO3 Expression Builder is enforcing that
        return (string) $quoted;
    }

    /**
     * Unquote a single identifier (no dot expansion). Used to unquote the table names
     * from the expressionBuilder so that the table can be found in the TCA definition.
     *
     * @param string $identifier The identifier / table name
     * @return string The unquoted table name / identifier
     */
    public function unquoteSingleIdentifier(string $identifier): string
    {
        $unquotedIdentifier = trim($identifier, '[]');
        return $unquotedIdentifier;
    }

    /**
     * Returns the ID of the last inserted row or sequence value.
     * If table and fieldname have been provided it tries to build
     * the sequence name for PostgreSQL. For MySQL the parameters
     * are not required / and only the table name is passed through.
     *
     * @param string|null $tableName
     * @param string $fieldName
     * @return string
     */
    public function lastInsertId($tableName = null, string $fieldName = 'uid'): string
    {
        $rows = $this->fetchAll(sprintf('SELECT IDENT_CURRENT(\'%s\') AS id', $tableName));
        return $rows[0]['id'] ?: '';
    }
}